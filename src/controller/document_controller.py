from src.controller.docling_controller import DoclingController
from src.controller.product_controller import ProductsDataController
from src.controller.weaviate_controller import WeaviateController

from src.schemas.weaviate import  DEFAULT_SCHEMA
from src.utils.file_loader import FileLoader
# from src.schemas.file_loader import DirectoryConfig
import os

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
        content,chunk_indexes, sources, image_urls_list, youtube_urls_list= self.processor.process()
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
            origin=origin,
            chunk_index=chunk_indexes
        )

    def insert_into_weaviate(self):
        """Process, chunk, and insert data into Weaviate."""
        print("start to processing....")
        content,chunk_indexes, sources = self.processor.process(
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
            origin=origin,
            chunk_index=chunk_indexes
        )

    # def insert_into_weaviate_prod_spec(self):
    #     """Process, chunk, and insert data into Weaviate."""
    #     print("start to processing....")
    #     content, sources = self.processor.process_product_spec(
    #         input_paths=self.file_loader.load_files()
    #     )
    #     level = [self.level]* len(sources)
    #     origin = [self.origin]* len(sources)
    #     print(f"Content: {content}", f"\n\nSources: {sources},\n\nConfidential Level: {self.level}")
    #     print(f"Content length: {len(content)}", f"Sources length: {len(sources)}")
    #     self.weaviate_client.insert_data_from_lists(
    #         content=content,
    #         source=sources,
    #         level=level,
    #         origin=origin
    #     )
    import os

    def insert_into_weaviate_prod_spec(self, batch_size=30, log_file="processed_files.txt", max_file_size_mb=10):
        """Process files in batches, skip files >10MB, insert into Weaviate, and log each batch."""
        print("Start processing...")

        # Load all files
        all_files = self.file_loader.load_files()

        # Read already processed files from log (if exists)
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                processed_files = set(line.strip() for line in f.readlines())
        except FileNotFoundError:
            processed_files = set()

        # Filter out files that are already processed
        files_to_process = [f for f in all_files if f not in processed_files]

        # Filter out files larger than max_file_size_mb
        filtered_files = []
        for f in files_to_process:
            try:
                size_mb = os.path.getsize(f) / (1024 * 1024)
                if size_mb <= max_file_size_mb:
                    filtered_files.append(f)
                else:
                    print(f"Skipping large file (> {max_file_size_mb}MB): {f}")
            except FileNotFoundError:
                print(f"File not found, skipping: {f}")

        total_files = len(filtered_files)
        print(f"Total files to process (<= {max_file_size_mb}MB): {total_files}")

        # Process in batches
        for i in range(0, total_files, batch_size):
            batch_files = filtered_files[i:i + batch_size]
            print(f"\nProcessing batch {i // batch_size + 1}: {batch_files}")

            # Process the batch
            content,chunk_indexes, sources = self.processor.process_product_spec(input_paths=batch_files)

            # Record the batch file names for each content piece
            source_with_file = []
            for idx, src in enumerate(sources):
                file_index = idx % len(batch_files)  # Map content to corresponding file
                source_with_file.append(f"{batch_files[file_index]}::{src}")

            # Prepare level and origin
            level_list = [self.level] * len(content)
            origin_list = [self.origin] * len(content)

            # Insert into Weaviate
            self.weaviate_client.insert_data_from_lists(
                content=content,
                source=source_with_file,
                level=level_list,
                origin=origin_list,
                chunk_index=chunk_indexes
            )

            # Log the processed batch immediately
            with open(log_file, "a", encoding="utf-8") as f_log:
                for file_path in batch_files:
                    f_log.write(file_path + "\n")
                    processed_files.add(file_path)  # Update in-memory set

            print(f"Batch {i // batch_size + 1} inserted and logged successfully!")

        print("\nAll batches processed and logged successfully!")





    def run(self):
        """Main execution method."""
        if self.product:
            self.insert_product_into_weaviate()
        else:
            self.insert_into_weaviate()
        
    def run_product_spec(self):
        """Main execution method."""
        self.insert_into_weaviate_prod_spec()

    def print_collection_info(self):
        """Print information about the Weaviate collection."""
        self.weaviate_client.print_collection_info()
        
    def retrieve_data_by_field(self, field_list, limit=5,filters=None):
        """Retrieve data from Weaviate by specified fields."""
        self.weaviate_client.retrieve_data_by_field(field_list, limit,filters)
    def query_data(self, query_text, limit=5):
        self.weaviate_client.query_data(query_text=query_text,limit=limit)
    def query_data_hybrid(self,query_text,limit=5,index_range=50):
        self.weaviate_client.query_data_hybrid(query_text=query_text,limit=limit,index_range=index_range)
        
