from src.controller.struct_to_pg import StructuredToPGController

if __name__ == "__main__":
    loader = StructuredToPGController(
        files_dir=[
            "sevensix_data/機密レベル2/営業本部/営業活動/名刺データ/eight20250612171719sjis_processed.csv"
        ],
        use_dask=True,
    )
    loader.process_files()