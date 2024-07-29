import requests
from bs4 import BeautifulSoup
import logging
from typing import Optional, List, Dict
import time
import random
import asyncio
import aiohttp
from aiohttp import ClientSession
import csv
from dataclasses import dataclass, asdict
from urllib.robotparser import RobotFileParser
import json
from concurrent.futures import ThreadPoolExecutor
import sqlite3

@dataclass
class ScrapedItem:
    title: str
    description: str
    url: str

class WebScraper:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self._setup_logging()
        self.robot_parser = RobotFileParser()
        self.robot_parser.set_url(f"{self.base_url}/robots.txt")
        self.robot_parser.read()
        self.db_connection = sqlite3.connect('scraped_data.db')
        self._setup_database()

    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def _setup_database(self):
        cursor = self.db_connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT,
                description TEXT,
                url TEXT UNIQUE
            )
        ''')
        self.db_connection.commit()

    def fetch_page(self, url: str) -> Optional[str]:
        if not self.robot_parser.can_fetch("*", url):
            self.logger.warning(f"Robots.txt disallows scraping: {url}")
            return None
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None

    def parse_page(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, 'html.parser')

    def extract_data(self, soup: BeautifulSoup, url: str) -> ScrapedItem:
        # Implement your data extraction logic here
        # This is a placeholder implementation
        title = soup.find('h1').text.strip() if soup.find('h1') else ''
        description = soup.find('meta', attrs={'name': 'description'})
        description = description['content'] if description else ''
        return ScrapedItem(title=title, description=description, url=url)

    async def async_fetch_page(self, url: str, session: ClientSession) -> Optional[str]:
        if not self.robot_parser.can_fetch("*", url):
            self.logger.warning(f"Robots.txt disallows scraping: {url}")
            return None
        
        try:
            async with session.get(url) as response:
                return await response.text()
        except aiohttp.ClientError as e:
            self.logger.error(f"Error fetching {url}: {e}")
            return None

    async def async_scrape(self, max_pages: int = 5) -> List[ScrapedItem]:
        urls = [f"{self.base_url}/page/{page}" for page in range(1, max_pages + 1)]
        async with aiohttp.ClientSession() as session:
            tasks = [self.async_fetch_page(url, session) for url in urls]
            pages = await asyncio.gather(*tasks)
        
        all_data = []
        for url, html in zip(urls, pages):
            if html:
                soup = self.parse_page(html)
                item = self.extract_data(soup, url)
                all_data.append(item)
                self._save_to_database(item)
        
        return all_data

    def _save_to_database(self, item: ScrapedItem):
        cursor = self.db_connection.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO scraped_items (title, description, url)
            VALUES (?, ?, ?)
        ''', (item.title, item.description, item.url))
        self.db_connection.commit()

    def export_to_csv(self, data: List[ScrapedItem], filename: str = 'scraped_data.csv'):
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['title', 'description', 'url']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in data:
                writer.writerow(asdict(item))

    def export_to_json(self, data: List[ScrapedItem], filename: str = 'scraped_data.json'):
        with open(filename, 'w', encoding='utf-8') as jsonfile:
            json.dump([asdict(item) for item in data], jsonfile, ensure_ascii=False, indent=2)

def main():
    base_url = "https://example.com"  # Replace with the actual website you want to scrape
    scraper = WebScraper(base_url)
    
    # Asynchronous scraping
    results = asyncio.run(scraper.async_scrape())
    
    # Export results
    scraper.export_to_csv(results)
    scraper.export_to_json(results)
    
    print(f"Scraped {len(results)} items")
    for item in results[:5]:  # Print first 5 items as an example
        print(item)

if __name__ == "__main__":
    main()