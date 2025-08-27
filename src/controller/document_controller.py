from src.controller.docling_controller import DoclingController
from src.controller.product_controller import ProductsDataController
from src.controller.weaviate_controller import WeaviateController

from src.schemas.weaviate import  DEFAULT_SCHEMA
from src.utils.file_loader import FileLoader
# from src.schemas.file_loader import DirectoryConfig


class DocumentController:
    """Processes WordPress data and inserts it into Weaviate."""
    def __init__(self,dir_path:list=[],collection_name:str="default",level:str = "1",origin:str="s3_bucket", embedding:str="openai",properties=DEFAULT_SCHEMA,collection_delete:bool=False,product:bool=False,allowed_extensions:list=[".pdf", ".docx", ".txt"]):
        self.weaviate_client = WeaviateController(
            collection_name=collection_name,
            embedding_provider=embedding,
            properties=properties,
            collection_delete=collection_delete
        )
        self.level = level
        self.origin = origin
        self.processor = None

        self.product=product
        self.process_document()
        self.file_loader = FileLoader(directory_paths=dir_path, allowed_extensions=allowed_extensions)
        
    def process_document(self):
        if self.product:
            self.processor = ProductsDataController()
        else:
            self.processor = DoclingController()

    def insert_product_into_weaviate(self):
        """Process, chunk, and insert data into Weaviate."""
        print("start to processing....")
        content, sources, image_urls_list, youtube_urls_list= self.processor.process()
        level = [self.level]* len(sources)
        origin = [self.origin]* len(sources)
        print(f"Content: {content}", f"\n\nSources: {sources},\n\nConfidential Level: {type(self.level)}")
        print(f"Content length: {len(content)}", f"Sources length: {len(sources)}")
        self.weaviate_client.insert_data_from_lists(
            content=content,
            source=sources,
            image_urls=image_urls_list, 
            youtube_urls=youtube_urls_list,
            level=level,
            origin=origin
        )

    def insert_into_weaviate(self):
        """Process, chunk, and insert data into Weaviate."""
        print("start to processing....")
        content, sources = self.processor.process(
            input_paths=self.file_loader.load_files()
        )
        level = [self.level]* len(sources)
        origin = [self.origin]* len(sources)
        print(f"Content: {content}", f"\n\nSources: {sources},\n\nConfidential Level: {self.level}")
        print(f"Content length: {len(content)}", f"Sources length: {len(sources)}")
        self.weaviate_client.insert_data_from_lists(
            content=content,
            source=sources,
            level=level,
            origin=origin
        )

    def run(self):
        """Main execution method."""
        if self.product:
            self.insert_product_into_weaviate()
        else:
            self.insert_into_weaviate()
    

    def print_collection_info(self):
        """Print information about the Weaviate collection."""
        self.weaviate_client.print_collection_info()
        
    def retrieve_data_by_field(self, field_list, limit=5):
        """Retrieve data from Weaviate by specified fields."""
        self.weaviate_client.retrieve_data_by_field(field_list, limit)
    def query_data(self, query_text, limit=5):
        self.weaviate_client.query_data(query_text=query_text,limit=limit)
    def query_data_hybrid(self,query_text,limit=5):
        self.weaviate_client.query_data_hybrid(query_text=query_text,limit=limit)
        
