from src.controller.document_controller import DocumentController
from src.schemas.weaviate import  PRODUCT_SCHEMA
from dotenv import load_dotenv
load_dotenv(override=True)
processor = DocumentController(level="1",collection_name="Product_data",properties=PRODUCT_SCHEMA,collection_delete=False,product=True)
processor.run()
processor.retrieve_data_by_field(
    field_list=["content", "source","level","source","image_urls","youtube_urls"],
    limit=20
)
# print(" near searach\n"," *"*50)
# processor.query_data("iqom")

print(" hybrid search\n"," *"*50)
processor.query_data_hybrid("iqom")


