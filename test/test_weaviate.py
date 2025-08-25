from src.controller.weaviate_controller import WeaviateController
from src.schemas.weaviate import Product_collection_property_config, Product_collection_tenancy_list
# from src
from dotenv import load_dotenv
load_dotenv()
weaviate_client = WeaviateController(
            collection_name="Regulation_data",
            embedding_provider="openai",
            #collection_delete=True
        )

# weaviate_client.retrieve_data_by_field(["source"], 125)
weaviate_client.query_data("に車輌の整備に心がけ、定期点検を怠", limit=1)

