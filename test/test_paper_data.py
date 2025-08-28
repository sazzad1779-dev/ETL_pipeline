from dotenv import load_dotenv
load_dotenv(override=True)
from src.controller.document_controller import DocumentController
dirs=["Sevensix_dropbox/機密レベル2/営業本部/論文データ"]

processor = DocumentController(dir_path=dirs,
                               level="2",
                               collection_name="Paper_data",
                               collection_delete=False)

# processor.run()
processor.retrieve_data_by_field(
    field_list=["content", "source"],
    limit=5
)
print(" near searach\n"," *"*50)
processor.query_data("10.4 bits/s/Hz.")
print(" hybrid search\n"," *"*50)
processor.query_data_hybrid("10.4 bits/s/Hz.")