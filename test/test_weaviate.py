from src.controller.weaviate_controller import WeaviateController
from src.schemas.weaviate import Product_collection_property_config, Product_collection_tenancy_list
# from src
from dotenv import load_dotenv
load_dotenv()
weaviate_client = WeaviateController(
            collection_name="Product_collection_demo",
            embedding_provider="openai",
            #collection_delete=True
        )

weaviate_client.retrieve_data_by_field(["content"], 25)
weaviate_client.query_data("レーザー冷却とは原子に特定の波長のレ", limit=1)

