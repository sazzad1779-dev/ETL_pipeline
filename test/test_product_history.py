from src.utils.structToDB.process_multi_table_in_sheet import read_multi_table_excel
from src.controller.postgres_controller import PostgresController

psql = PostgresController()
# Usage
file_path = "Sevensix_dropbox/機密レベル3/企画管理本部/業務推進部/売上台帳/18期_売上_納期管理台帳.xlsx"
df = read_multi_table_excel(file_path, sheet_name="18期売上台帳", key_column="担当")

psql.insert_df(df,"Product_history_data")

