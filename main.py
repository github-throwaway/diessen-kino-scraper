import asyncio
import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Optional
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup
from icalendar import Calendar, Event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()],
)


class FastMovieScraper:
    def __init__(self, base_url="https://www.kino-diessen.de", max_concurrent=50):
        self.base_url = base_url
        self.location = "Kinowelt am Ammersee\nFischerei 12\n86911 Dießen am Ammersee"
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(max_concurrent)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *exc):
        await self.session.close()

    @staticmethod
    def _create_event_description(movie_details: Dict, booking_url: str) -> str:
        description_lines = [
            movie_details.get("description", ""),
            "",
            f"🕐 Dauer: {movie_details.get('duration', 120)} Minuten",
            f"🔞 FSK: {movie_details.get('fsk', '')}"
            if movie_details.get("fsk")
            else None,
            f"🏛️ Saal: {movie_details.get('room', '')}"
            if movie_details.get("room")
            else None,
            "",
            f"🎬 Trailer: {movie_details.get('trailer', '')}"
            if movie_details.get("trailer")
            else None,
            "",
            f"🎟️ Tickets: {booking_url}",
        ]
        return "\n\n".join(filter(None, description_lines))

    async def get_movie_details(self, url: str, movie_title: str) -> Dict:
        async with self.semaphore:
            try:
                async with self.session.get(urljoin(self.base_url, url)) as response:
                    response.raise_for_status()
                    html = await response.text()
                    soup = BeautifulSoup(html, "html.parser")

                    details = {
                        "duration": 120,
                        "description": "",
                        "trailer": "",
                        "fsk": "",
                        "room": "",
                    }

                    desc_elem = soup.select_one(
                        "#sp-component div.cal-data-reduced div.col div ul li:nth-child(4)"
                    )
                    details["description"] = desc_elem.text.strip() if desc_elem else ""

                    duration_elem = soup.select_one(
                        "#sp-component div.cal-data-reduced div.col div ul li:nth-child(2)"
                    )
                    if duration_elem:
                        text = duration_elem.text.strip()
                        duration_match = re.search(r"(\d+)\s*Min\.", text)
                        details["duration"] = (
                            int(duration_match.group(1)) if duration_match else 120
                        )

                        fsk_match = re.search(r"FSK:\s*(\d+)", text)
                        details["fsk"] = fsk_match.group(1) if fsk_match else ""

                    room_elem = soup.select_one(
                        "#sp-component div.cal-data-performance div:nth-child(1) span:nth-child(3)"
                    )
                    details["room"] = room_elem.text.strip() if room_elem else ""

                    trailer_elem = soup.find(
                        "iframe", src=lambda s: s and "youtube.com" in s
                    )
                    details["trailer"] = trailer_elem["src"] if trailer_elem else ""

                    logging.info(f"Processed movie details: {movie_title}")
                    return details

            except Exception as e:
                logging.error(f"Error fetching movie details for {movie_title}: {e}")
                return {}

    async def scrape_movies(self) -> Optional[Calendar]:
        start_time = time.time()
        try:
            async with self.session.get(
                f"{self.base_url}/im-kino-offcanvas/kinoprogramm/"
            ) as response:
                response.raise_for_status()
                cal = self._prepare_calendar()

                soup = BeautifulSoup(await response.text(), "html.parser")
                table = soup.find("table", class_="table-text")

                if not table:
                    logging.error("No movie table found")
                    return None

                dates, movies = self._extract_table_data(table)
                movie_details = await self._fetch_movie_details(movies)
                events = await self._generate_movie_events(movies, dates, movie_details)

                self._add_events_to_calendar(cal, events)

                end_time = time.time()
                logging.info(
                    f"Total scraping time: {end_time - start_time:.2f} seconds"
                )
                return cal

        except Exception as e:
            logging.error(f"Error scraping movies: {e}")
            return None

    @staticmethod
    def _prepare_calendar() -> Calendar:
        cal = Calendar()
        cal.add("version", "2.0")
        cal.add("prodid", "-//Kino Dießen Movie Schedule//diessen.de//")
        return cal

    def _extract_table_data(self, table):
        dates = [
            self._parse_date(th.text.strip().replace("\n", ""))
            for th in table.find("thead").find_all("th")[1:]
        ]
        movies = table.find("tbody").find_all("tr")
        return dates, movies

    async def _fetch_movie_details(self, movies):
        movie_details_tasks = []
        for row in movies:
            movie_link = row.find("a")
            if movie_link:
                movie_url = movie_link["href"]
                movie_title = f"🎬 {movie_link.text.strip()}"
                movie_details_tasks.append(
                    self.get_movie_details(movie_url, movie_title)
                )
        return await asyncio.gather(*movie_details_tasks)

    async def _generate_movie_events(self, movies, dates, movie_details):
        async def process_movie(row, details):
            movie_link = row.find("a")
            if not movie_link or not details:
                return None

            movie_title = f"🎬 {movie_link.text.strip()}"
            events = []
            screenings = row.find_all("td")

            for date, cell in zip(dates, screenings):
                for time_link in cell.find_all("a"):
                    event = self._create_movie_event(
                        movie_title, date, time_link, details
                    )
                    events.append(event)

            return events

        event_tasks = [
            process_movie(row, details) for row, details in zip(movies, movie_details)
        ]
        return await asyncio.gather(*event_tasks)

    def _create_movie_event(self, movie_title, date, time_link, details):
        time_text = time_link.find("span").text.strip()
        hour, minute = map(int, time_text.split(":"))

        event = Event()
        event.add("summary", movie_title)
        start_time = date.replace(hour=hour, minute=minute)
        event.add("dtstart", start_time)
        event.add("dtend", start_time + timedelta(minutes=details["duration"]))
        event.add("location", self.location)

        description = self._create_event_description(details, time_link["href"])
        event.add("description", description)
        return event

    @staticmethod
    def _add_events_to_calendar(cal, results):
        for movie_events in results:
            if movie_events:
                for event in movie_events:
                    cal.add_component(event)

    @staticmethod
    def _parse_date(date_text: str) -> datetime:
        match = re.search(r"(\d{2})\.(\d{2})\.", date_text)
        return (
            datetime(datetime.now().year, int(match.group(2)), int(match.group(1)))
            if match
            else None
        )

    async def save_calendar(self, cal: Calendar):
        try:
            os.makedirs("docs", exist_ok=True)

            with open("docs/movies.ics", "wb") as f:
                f.write(cal.to_ical())
            logging.info("Calendar file saved successfully")

            await self._create_html_page()
        except Exception as e:
            logging.error(f"Error saving calendar: {e}")
            raise

    @staticmethod
    async def _create_html_page():
        try:
            with open("template.html", "r", encoding="utf-8") as template_file:
                template_content = template_file.read()

            updated_html = template_content.replace(
                "{{LAST_UPDATED}}", datetime.now().strftime("%d.%m.%Y %H:%M")
            )

            with open("docs/index.html", "w", encoding="utf-8") as output_file:
                output_file.write(updated_html)

            logging.info("HTML page updated successfully")
        except Exception as e:
            logging.error(f"Error creating HTML page: {e}")
            raise


async def main():
    async with FastMovieScraper() as scraper:
        cal = await scraper.scrape_movies()
        if cal:
            await scraper.save_calendar(cal)


if __name__ == "__main__":
    asyncio.run(main())
