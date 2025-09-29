from src.controller.document_controller import DocumentController
from src.schemas.weaviate import  PRODUCT_SCHEMA
from weaviate.classes.query import Filter
from src.utils.weaviate_monitoring import weaviate_monitor
from dotenv import load_dotenv
load_dotenv(override=True)
processor = DocumentController(level="1",collection_name="Product_data",properties=PRODUCT_SCHEMA,collection_delete=False,product=True)
# processor.run()
# processor.retrieve_data_by_field(
#     field_list=["content", "source", "level", "image_urls", "youtube_urls", "origin", "chunk_index"],
#     limit=20,
#     filters=Filter.by_property("source").equal("https://www.sevensix.co.jp/products/superk-fianium"),
# )
# processor.delete_data_by_source()


# processor.update_by_fields(uuid="")
print(" hybrid search\n"," *"*50)
processor.query_data_hybrid("tell me about facial recognation ", limit=50,index_range=5)




