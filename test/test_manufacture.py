from src.utils.structToDB.process_multi_xlsx_to_single_table import combine_excel_files
from src.controller.postgres_controller import PostgresController

psql = PostgresController()
# Example usage:
df = combine_excel_files(
    "Sevensix_dropbox/機密レベル3/企画管理本部/業務推進部/売上台帳", 
    # sheet_name="SalesData",
    skip_files=["18期_売上_納期管理台帳.xlsx"]
)
psql.insert_df(df,"manufacturer_history")