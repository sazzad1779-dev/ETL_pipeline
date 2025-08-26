from src.controller.structured_data_controller import StructuredToPGController

if __name__ == "__main__":
    loader = StructuredToPGController(
        files_dir=[
            "Sevensix_dropbox/機密レベル2/営業本部/営業活動/名刺データ/Eight20250612171719sjis.csv"
        ],
        use_dask=False,
    )
    loader.process_files()