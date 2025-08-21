from src.controller.document_controller import DocumentController
# from src.schemas.file_loader import DirectoryConfig
# dirs = [
#     DirectoryConfig("Sevensix_dropbox/機密レベル2/企画管理本部/社内規程・就業規則", "Regulation_data", 2),
#     DirectoryConfig("Sevensix_dropbox/機密レベル2/営業本部/論文データ", "Paper_data", 2),
# ]
# processor = DocumentController(directories=dirs,collection_delete=True)

dirs=["Sevensix_dropbox/機密レベル2/企画管理本部/社内規程・就業規則"]
# dirs=["Sevensix_dropbox/機密レベル2/営業本部/論文データ"]

processor = DocumentController(dir_path=dirs,level=2,collection_name="Regulation_data",collection_delete=True)

processor.run()
processor.retrieve_data_by_field(
    field_list=["content", "source","level"],
    limit=5
)
