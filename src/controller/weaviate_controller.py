import os
import weaviate
from weaviate.classes.config import Configure, Property
# from src.utils.vectorDB.weaviate_utils import run_query, delete_by_source, print_collection_info,retrieve_by_field
# from src.config import property_config
from src.schemas.weaviate import property_config
from src.utils.vectorDB.weaviate_utils import WeaviateUtils

class WeaviateController:
    def __init__(
        self,
        collection_name:str="DemoCollection",
        properties=None,
        embedding_provider:str="openai",
        embedding_model:str=None,
        tenancy_list:list=None,
        collection_delete:bool=False
    ):
        self.collection_name = collection_name
        self.embedding_provider = embedding_provider.lower()
        self.embedding_model = embedding_model
        self.collection_delete = collection_delete
        self.properties_config = properties or property_config
        self.tenancy_list = tenancy_list or []
        self.client = self._connect()
        self.collection = self._create_collection()
        self.weaviate_utils = WeaviateUtils(self.collection)


    def _connect(self):
        headers = {}
        if self.embedding_provider == "jina":
            headers["X-JinaAI-Api-Key"] = os.getenv("JINAAI_API_KEY")
        elif self.embedding_provider == "openai":
            headers["X-OpenAI-Api-Key"] = os.getenv("OPENAI_API_KEY")
        return weaviate.connect_to_local(headers=headers)

    def _vector_config(self):
        vectorize_props = [p["name"] for p in self.properties_config if p.get("vectorize_property")]
        if self.embedding_provider == "jina":
            return Configure.Vectors.text2vec_jinaai(
                name="text_vector",
                model=self.embedding_model or "jina-embeddings-v3",
                source_properties=vectorize_props
            )
        elif self.embedding_provider == "openai":
            return Configure.Vectors.text2vec_openai(
                name="text_vector",
                model=self.embedding_model or "text-embedding-3-small",
                source_properties=vectorize_props
            )
        raise ValueError(f"Unsupported embedding provider: {self.embedding_provider}")

    def _create_collection(self):
        if self.collection_delete:
            print(f"Deleting existing collection '{self.collection_name}'...")
            self.client.collections.delete(self.collection_name)

        if self.collection_name in self.client.collections.list_all():
            print(f"Collection '{self.collection_name}' already exists. Using existing collection.")
            return self.client.collections.get(self.collection_name)

        print(f"Creating collection '{self.collection_name}'...")
        properties_list = [Property(**prop) for prop in self.properties_config]
        collection = self.client.collections.create(
            self.collection_name,
            vector_config=self._vector_config(),
            properties=properties_list
        )
        if self.tenancy_list:
            collection.tenants.create(self.tenancy_list)
            print(f"Tenant information: {collection.tenants.get()}")
        return collection

    def insert_data_from_lists(self, **kwargs):
        self.weaviate_utils.insert_data(**kwargs )
    # --- Methods calling utils ---
    def query_data(self, query_text, limit=5):
        self.weaviate_utils.run_query(query_text, limit)

    def query_data_hybrid(self, query_text, limit=5):
        self.weaviate_utils.run_query_hybrid(query_text, limit)
    
    def retrieve_data_by_field(self, field_list:list, limit:int=5):
        """Retrieve data using a near_text query."""
        self.weaviate_utils.retrieve_by_field(field_list, limit)

    def delete_data_by_source(self, file_source: str):
        """Delete data by source."""
        self.weaviate_utils.delete_by_source( file_source)

    def show_collection_info(self):
        """Print collection metadata."""
        self.weaviate_utils.print_collection_info(self.collection)
