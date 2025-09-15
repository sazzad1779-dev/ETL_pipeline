from dotenv import load_dotenv
load_dotenv(override=True)
from src.controller.document_controller import DocumentController

from src.schemas.weaviate import  DEFAULT_SCHEMA
# dirs=["Sevensix_dropbox/機密レベル2/企画管理本部"]
dirs=["Sevensix_dropbox/機密レベル2/企画管理本部/社内規程・就業規則/4_規程_経理"]

processor = DocumentController(dir_path=dirs,level="2",collection_name="Regulation_data",properties=DEFAULT_SCHEMA,origin="s3_bucket",collection_delete=False)

processor.run()
processor.retrieve_data_by_field(
    field_list=["content", "source","level"],
    limit=5
)
# print(" near searach\n"," *"*50)
# processor.query_data("計単位は")
print(" hybrid search\n"," *"*50)
processor.query_data_hybrid("計単位は")