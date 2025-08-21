from sqlalchemy import create_engine, text
from bs4 import BeautifulSoup
import os

class ProductContentCleaner:
    """Handles database fetching and HTML cleaning for WordPress content."""

    def __init__(self):
        db_url = os.getenv("SOURCE_DB_URL","http://localhost:8080")  # ✅ Ensure this is set in your .env file
        if not db_url:
            raise ValueError("DB_URL environment variable not set")
        self.engine = create_engine(db_url, echo=False)
        

    def fetch_products(self):
        """Fetch product posts from WordPress DB."""
        query = text(f"""
            SELECT
                p.post_content as post_content,
                p.post_title as post_title,
                pm.meta_value as post_summary,
                 p.post_name as post_sub_url
            FROM
                wp_posts AS p
            JOIN
                wp_postmeta AS pm ON p.ID = pm.post_id
            WHERE
                p.post_type = 'products'
                AND pm.meta_key = 'cfp_fname_1' limit 10 ;
        """)

        with self.engine.connect() as connection:
            return connection.execute(query).fetchall()

    @staticmethod
    def clean_and_extract(html_content):
        """Strips HTML tags and extracts image and YouTube URLs."""
        clean_text = ""
        image_urls = []
        youtube_urls = []

        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Extract image URLs
            image_urls = [img['src'] for img in soup.find_all('img', src=True)]

            # Extract YouTube URLs from iframes
            youtube_urls += [
                iframe['src'] for iframe in soup.find_all('iframe', src=True)
                if 'youtube.com' in iframe['src']
            ]

            # Extract YouTube URLs from <div class="wp-block-embed__wrapper">
            youtube_urls += [
                div.get_text(strip=True)
                for div in soup.find_all('div', class_='wp-block-embed__wrapper')
                if 'youtube.com' in div.get_text(strip=True)
            ]

            # Extract YouTube URLs from <a> tags
            youtube_urls += [
                a['href'] for a in soup.find_all('a', href=True)
                if 'youtube.com' in a['href']
            ]

            # Get clean text without HTML
            clean_text = soup.get_text(separator=' ', strip=True)

        return clean_text, list(set(image_urls)), list(set(youtube_urls))
