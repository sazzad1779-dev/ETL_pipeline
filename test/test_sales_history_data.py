from src.controller.structured_data_controller import StructuredDataController
from src.controller.postgres_controller import PostgresController

psql = PostgresController()
source_dir = "Sevensix_dropbox/機密レベル3/企画管理本部/業務推進部/売上台帳"

controller = StructuredDataController(
    files_dir=[source_dir],
    allowed_extensions=[".xlsx",".xlsm",".csv"],
    use_dask=False,
    level="3",
    origin="s3_bucket",
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
    psql.insert_df(df_aligned, "sales_history", index=True)