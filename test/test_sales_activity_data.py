from dotenv import load_dotenv
load_dotenv(override=True)
from src.controller.postgres_controller import PostgresController
from src.utils.structToDB.process_xlsx_xlsm import ExcelDataExtractor

psql = PostgresController()
source_dir = "Sevensix_dropbox/機密レベル2/営業本部/営業活動/営業報告書"
extractor = ExcelDataExtractor()
file_list = extractor.list_excel_files(source_dir)
results = extractor.batch_process(files=file_list,level="2",origin="s3_bucket")
psql.insert_df(results,"sales_activity",index=True)