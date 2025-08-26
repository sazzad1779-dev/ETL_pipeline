from src.controller.document_controller import DocumentController
processor = DocumentController(dir_path="Sevensix_dropbox/機密レベル2/営業本部/論文データ",level="2",collection_name="Paper_data",collection_delete=True)

processor.run()
processor.retrieve_data_by_field(
    field_list=["content", "source","level"],
    limit=5
)