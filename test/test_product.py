from src.controller.product_controller import ProductsDataController
from src.controller.document_controller import DocumentController
from src.schemas.weaviate import  Product_collection_property_config
from dotenv import load_dotenv
load_dotenv()

# dirs=["Sevensix_dropbox/機密レベル2/企画管理本部/社内規程・就業規則"]
# dirs=["Sevensix_dropbox/機密レベル2/営業本部/論文データ"]

processor = DocumentController(level="1",collection_name="Product_data",properties=Product_collection_property_config,collection_delete=False,product=True)

# processor.run()
# processor.retrieve_data_by_field(
#     field_list=["content", "source","level","source","image_urls","youtube_urls"],
#     limit=20
# )
print(" near searach\n"," *"*50)
processor.query_data("対象サイズ")
print(" hybrid search\n"," *"*50)


processor.query_data_hybrid("対象サイズ")


