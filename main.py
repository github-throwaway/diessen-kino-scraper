import logging
import re

from bs4 import BeautifulSoup
import requests
from icalendar import Calendar, Event
from datetime import datetime, timedelta
import os
from urllib.parse import urljoin
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)


class MovieScraper:
    def __init__(self, base_url="https://www.kino-diessen.de"):
        self.base_url = base_url
        self.session = requests.Session()
        self.location = ("Kinowelt am Ammersee\nFischerei 12\n86911 Dießen am "
                         "Ammersee")

    def create_event_description(self, movie_details, booking_url):
        description_lines = [
            movie_details['description'],
            "",
            f"🕐 Dauer: {movie_details['duration']} Minuten" if movie_details[
                'duration'] else None,
            f"🔞 FSK: {movie_details['fsk']}" if movie_details['fsk'] else
            None,
            f"🏛️ Saal: {movie_details['room']}" if movie_details[
                'room'] else None,
            "",
            f"🎬 Trailer: {movie_details['trailer']}" if movie_details[
                'trailer'] else None,
            "",
            f"🎟️ Tickets: {booking_url}"
        ]
        return "\n\n".join(filter(None, description_lines))

    def get_movie_details(self, url):
        try:
            response = self.session.get(urljoin(self.base_url, url))
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            details = {
                'duration': 120,  # Default duration in minutes if not found
                'description': '',
                'trailer': '',
                'cast': '',
                'director': '',
                'fsk': '',
                'room': ''
            }

            # Extract description
            description_element = soup.select_one(
                '#sp-component > div > div.cal-data-reduced > div > div.col '
                '> div:nth-child(1) > div > ul > li:nth-child(4)'
            )
            if description_element:
                details['description'] = description_element.text.strip()

            # Extract duration and FSK rating
            duration_fsk_element = soup.select_one(
                '#sp-component > div > div.cal-data-reduced > div > div.col '
                '> div:nth-child(1) > div > ul > li:nth-child(2)'
            )
            if duration_fsk_element:
                text = duration_fsk_element.text.strip()
                if 'Min.' in text:
                    duration_text = text.split('Min.')[0].strip()
                    details['duration'] = int(
                        ''.join(filter(str.isdigit, duration_text))) or 120
                if 'FSK:' in text:
                    details['fsk'] = re.search(r'FSK:\s*(\d+)', text).group(
                        1) if re.search(r'FSK:\s*(\d+)', text) else ''

            # Extract room information
            room_element = soup.select_one(
                '#sp-component > div > div.cal-data-reduced > div > div.col '
                '> div.row.cal-data-performance > div:nth-child(1) > '
                'div:nth-child(1) > span:nth-child(3)'
            )
            if room_element:
                details['room'] = room_element.text.strip()

            # Extract cast
            cast_element = soup.select_one(
                '#sp-component > div > div.cal-data-reduced > div > div.col '
                '> div:nth-child(1) > div > ul > li:nth-child(1)'
            )
            if cast_element:
                text = cast_element.text.strip()
                if 'Mit:' in text:
                    details['cast'] = text.replace('Mit:', '').strip()

            # Extract director
            director_element = soup.select_one(
                '#sp-component > div > div.cal-data-reduced > div > div.col '
                '> div:nth-child(1) > div > ul > li:nth-child(3)'
            )
            if director_element:
                text = director_element.text.strip()
                if 'Regie:' in text:
                    details['director'] = text.replace('Regie:', '').strip()

            # Extract trailer URL
            if iframe := soup.find('iframe',
                                   src=lambda s: s and 'youtube.com' in s):
                details['trailer'] = iframe['src']

            return details
        except Exception as e:
            logging.error(f"Error fetching movie details for {url}: {e}")
            raise

    def scrape_movies(self):
        try:
            response = self.session.get(
                f"{self.base_url}/im-kino-offcanvas/kinoprogramm/")
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            cal = Calendar()
            cal.add('version', '2.0')
            cal.add('prodid', '-//Kino Dießen Movie Schedule//diessen.de//')

            table = soup.find('table', class_='table-text')
            if not table:
                logging.error("No movie table found")
                raise

            dates = []
            for th in table.find('thead').find_all('th')[1:]:
                date_text = th.text.strip().replace('\n', '')
                dates.append(self.parse_date(date_text))

            movies = table.find('tbody').find_all('tr')
            for row in tqdm(movies, desc="Processing movies"):
                movie_link = row.find('a')
                if not movie_link:
                    continue

                movie_url = movie_link['href']
                movie_title = f"🎬 {movie_link.text.strip()}"
                logging.info(f"Processing movie: {movie_title}")

                movie_details = self.get_movie_details(movie_url)
                if not movie_details:
                    logging.warning(
                        f"Skipping movie due to missing details: "
                        f"{movie_title}")
                    continue

                screenings = row.find_all('td')
                for date, cell in zip(dates, screenings):
                    for time_link in cell.find_all('a'):
                        time_text = time_link.find('span').text.strip()
                        hour, minute = map(int, time_text.split(':'))

                        event = Event()
                        event.add('summary', movie_title)
                        start_time = date.replace(hour=hour, minute=minute)
                        event.add('dtstart', start_time)
                        event.add('dtend', start_time + timedelta(
                            minutes=movie_details['duration']))
                        event.add('location', self.location)

                        description = self.create_event_description(
                            movie_details, time_link['href'])
                        event.add('description', description)
                        cal.add_component(event)

            return cal
        except Exception as e:
            logging.error(f"Error scraping movies: {e}")
            return None

    @staticmethod
    def parse_date(date_text):
        # Use regex to extract day and month (e.g., "23.01." -> day=23,
        # month=1)
        match = re.search(r'(\d{2})\.(\d{2})\.', date_text)
        if match:
            day, month = map(int, match.groups())
            return datetime(datetime.now().year, month, day)

    def save_calendar(self, cal):
        try:
            if not os.path.exists('docs'):
                os.makedirs('docs')

            with open('docs/movies.ics', 'wb') as f:
                f.write(cal.to_ical())
            logging.info("Calendar file saved successfully")

            self.create_html_page()
        except Exception as e:
            logging.error(f"Error saving calendar: {e}")
            raise

    def create_html_page(self):
        with open('docs/index_template.html', 'r',
                  encoding='utf-8') as template_file:
            html = template_file.read()

        # Optional: If you want to inject dynamic content
        html = html.replace('{{LAST_UPDATED}}',
                            datetime.now().strftime('%d.%m.%Y %H:%M'))

        with open('docs/index.html', 'w', encoding='utf-8') as f:
            f.write(html)


if __name__ == "__main__":
    scraper = MovieScraper()
    cal = scraper.scrape_movies()
    if cal:
        scraper.save_calendar(cal)
