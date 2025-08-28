from dotenv import load_dotenv
load_dotenv(override=True)
from src.controller.postgres_controller import PostgresController
from src.utils.structToDB.process_xlsx_xlsm import ExcelDataExtractor
from src.controller.structured_data_controller import StructuredDataController

import os


class Main:
    def __init__(self):
        self.psql = PostgresController()

    def sales_activity(self,source_dir:str="",origin:str="s3_bucket",level:str="2"):
        extractor = ExcelDataExtractor()
        file_list = extractor.list_excel_files(source_dir)
        results = extractor.batch_process(files=file_list,level=level,origin=origin)
        self.psql.insert_df(results,"sales_activity",index=True)

    def sales_history(self,source_dir:str="",origin:str="s3_bucket",level:str="2"):
        controller = StructuredDataController(
            files_dir=[source_dir],
            allowed_extensions=[".xlsx",".xlsm",".csv"],
            use_dask=False,
            level=level,
            origin=origin,
            skip_files = ["18期_売上_納期管理台帳.xlsx","19期_売上_納期管理台帳（マクロ）.xlsm"]
        )
        processed_data = controller.process_files()
        all_columns = set()
        for df in processed_data.values():
            all_columns.update(df.columns)
        all_columns = list(all_columns)  # final column order

        # 2. Reindex each df so they all match the same schema
        for table_name, df in processed_data.items():
            # Align columns with the union schema
            df_aligned = df.reindex(columns=all_columns)
            # Force everything to string to avoid type mismatch
            df_aligned = df_aligned.astype(str)
            # Insert into sales_history
            self.psql.insert_df(df_aligned, "sales_history", index=True)

    def business_data(self,source_dir:str="",origin:str="s3_bucket",level:str="2"):
        from src.controller.agentic_controller import AgenticExtractor
        extractor = AgenticExtractor()
        i=0
        # Loop through all files in the directory (non-recursive)
        for filename in os.listdir(source_dir):
            if filename.lower().endswith(".pdf"):
                file_path = os.path.join(source_dir, filename)
                print("Found PDF:", file_path)
                result = extractor.parse_documents(file_path)
                self.psql.create_tables()
                ser_result = result[0].extraction
                print(ser_result)
                # Insert the structured data into the Postgres database
                self.psql.insert_organization_person(ser_result,source=file_path,origin=origin,level=level)

    def person_data(self,source_dir:str="",origin:str="s3_bucket",level:str="2"):
        controller = StructuredDataController(
            files_dir=[source_dir],
            allowed_extensions=[".csv"],
            use_dask=False,
            level=level,
            origin=origin,
            skip_files = []
        )
        processed_data = controller.process_files()

        all_columns = set()
        for df in processed_data.values():
            all_columns.update(df.columns)
        all_columns = list(all_columns)  # final column order
        # 2. Reindex each df so they all match the same schema
        for table_name, df in processed_data.items():
            # Align columns with the union schema
            df_aligned = df.reindex(columns=all_columns)
            # Force everything to string to avoid type mismatch
            df_aligned = df_aligned.astype(str)
            # Insert into sales_history
            self.psql.insert_df(df_aligned, "person_data", index=True)


main = Main()

business_data_dir = "Sevensix_dropbox/機密レベル3/企画管理本部/人事総務部/競合企業情報"
sales_history_dir = "Sevensix_dropbox/機密レベル3/企画管理本部/業務推進部/売上台帳"
sales_activity_dir = "Sevensix_dropbox/機密レベル2/営業本部/営業活動/営業報告書"
person_data_dir = "Sevensix_dropbox/機密レベル2/営業本部/営業活動/名刺データ"
# main.sales_activity(sales_activity_dir,level="2",origin="s3_bucket")
# main.sales_history(sales_history_dir,level="3",origin="s3_bucket")
# main.person_data(person_data_dir,level="2",origin="s3_bucket")
main.business_data(business_data_dir,level="3",origin="s3_bucket")

