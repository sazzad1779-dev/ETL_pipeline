import os
from src.utils.data_processor.product_data import ProductContentCleaner
from src.utils.chunker import TextChunker

class ProductsDataController:
    """Coordinates fetching and cleaning of WordPress product data."""

    def __init__(self): 
        self.cleaner = ProductContentCleaner()
        self.chunker = TextChunker(chunk_size=1000, chunk_overlap=200)
    def process(self):
        """Fetch, clean, and chunk WordPress product data."""
        rows = self.cleaner.fetch_products()
        chunked_texts = []
        source_urls_list = []
        image_urls_list = []
        youtube_urls_list = []

        for row in rows:
            cleaned_content, image_urls, youtube_urls = self.cleaner.clean_and_extract(row.post_content)

            image_urls_str = "\n".join(image_urls) if image_urls else ""
            youtube_urls_str = "\n".join(youtube_urls) if youtube_urls else ""

            full_text = f"Title: {row.post_title}\nSummary:{row.post_summary} \nDesciption: {cleaned_content}"
            text_chunks = self.chunker.split_texts(full_text)

            chunked_texts.extend(text_chunks)
            source_urls_list.extend([f"https://www.sevensix.co.jp/products/{row.post_sub_url}"] * len(text_chunks))
            image_urls_list.extend([image_urls_str] * len(text_chunks))
            youtube_urls_list.extend([youtube_urls_str] * len(text_chunks))

        return chunked_texts, source_urls_list, image_urls_list, youtube_urls_list

if __name__ == "__main__":
    processor = ProductsDataController()
    processor.process()
