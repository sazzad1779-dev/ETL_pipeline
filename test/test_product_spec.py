from dotenv import load_dotenv
load_dotenv(override=True)
from src.controller.document_controller import DocumentController
dirs="Sevensix_dropbox/機密レベル3/営業本部/メーカー別"

processor = DocumentController(dir_path=[dirs],
                               level="2",
                               collection_name="Product_specification_data",
                               collection_delete=True,
                               allowed_extensions=[".pdf"])#".pptx",".doc",".docx",])

processor.run_product_spec()
processor.retrieve_data_by_field(
    field_list=["content", "source","level"],
    limit=5
)
# print(" near searach\n"," *"*50)
# processor.query_data("対象サイズ")
# print(" hybrid search\n"," *"*50)
# processor.query_data_hybrid("対象サイズ")