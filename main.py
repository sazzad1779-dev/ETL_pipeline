import json
import os
import sys
import traceback
from dotenv import load_dotenv
load_dotenv(override=True)

import traceback
from sqlalchemy import text
# === Controllers ===
from src.controller.document_controller import DocumentController
from src.schemas.weaviate import PRODUCT_SCHEMA, DEFAULT_SCHEMA
from src.controller.postgres_controller import PostgresController
from src.utils.structToDB.process_xlsx_xlsm import ExcelDataExtractor
from src.controller.structured_data_controller import StructuredDataController

# Optional Agentic extractor
try:
    from src.controller.agentic_controller import AgenticExtractor
except ImportError:
    AgenticExtractor = None


# --------------------------------------------
# CONFIG
# --------------------------------------------
SCHEMA_MAP = {
    "PRODUCT_SCHEMA": PRODUCT_SCHEMA,
    "DEFAULT_SCHEMA": DEFAULT_SCHEMA
}


# --------------------------------------------
# Structured Data Controller Wrapper
# --------------------------------------------
class StructuredRunner:
    def __init__(self):
        self.psql = PostgresController()
        self.psql.delete_all_collections(confirm=True)
    # --------------------------------------------
    # Database Connection Checks
    # --------------------------------------------
    def check_postgres_connection(self) -> bool:
        try:
            if hasattr(self.psql, "engine"):
                with self.psql.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                print("✅ PostgreSQL connection verified via SQLAlchemy.")
                return True
            elif hasattr(self.psql, "get_connection"):
                conn = self.psql.get_connection()
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                print("✅ PostgreSQL connection verified via psycopg2.")
                return True
            else:
                print("⚠️ PostgresController does not expose engine or get_connection().")
                return False
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
        return False


    def check_weaviate_connection(self) -> bool:
        try:
            # Try local or remote based on env
            weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080")
            print(f"Connecting to Weaviate instance at {weaviate_url} ...")

            from weaviate import connect_to_local, connect_to_weaviate_cloud
            from weaviate.classes.init import Auth

            if "localhost" in weaviate_url or "127.0.0.1" in weaviate_url:
                client = connect_to_local()
            else:
                api_key = os.getenv("WEAVIATE_API_KEY")
                client = connect_to_weaviate_cloud(
                    cluster_url=weaviate_url,
                    auth_credentials=Auth.api_key(api_key),
                )

            if client.is_ready():
                print("✅ Weaviate connection verified.")
                client.close()
                return True
            else:
                print("⚠️ Weaviate client not ready.")
                return False
        except Exception as e:
            print(f"❌ Weaviate connection failed: {e}")
            return False


    # --------------------------------------------
    # Structured Tasks
    # --------------------------------------------
    def sales_activity(self, source_dir, origin="s3_bucket", level="2"):
        extractor = ExcelDataExtractor()
        try:
            file_list = extractor.list_excel_files(source_dir)
            results = extractor.batch_process(files=file_list, level=level, origin=origin)
            self.psql.insert_df(results, "sales_activity", index=True)
            print("✅ sales_activity processed successfully.")
        except Exception as e:
            print(f"❌ Error in sales_activity: {e}")
            traceback.print_exc()

    def sales_history(self, source_dir, origin="s3_bucket", level="2"):
        try:
            controller = StructuredDataController(
                files_dir=[source_dir],
                allowed_extensions=[".xlsx", ".xlsm", ".csv"],
                use_dask=False,
                level=level,
                origin=origin,
                skip_files=["18期_売上_納期管理台帳.xlsx", "19期_売上_納期管理台帳（マクロ）.xlsm"]
            )
            processed_data = controller.process_files()
            all_columns = {col for df in processed_data.values() for col in df.columns}

            for _, df in processed_data.items():
                df_aligned = df.reindex(columns=all_columns).astype(str)
                self.psql.insert_df(df_aligned, "sales_history", index=True)
            print("✅ sales_history processed successfully.")
        except Exception as e:
            print(f"❌ Error in sales_history: {e}")
            traceback.print_exc()

    def business_data(self, source_dir, origin="s3_bucket", level="2"):
        if not AgenticExtractor:
            print("⚠️ AgenticExtractor not available, skipping business_data.")
            return
        try:
            extractor = AgenticExtractor()
            for filename in os.listdir(source_dir):
                if filename.lower().endswith(".pdf"):
                    file_path = os.path.join(source_dir, filename)
                    print(f"Found PDF: {file_path}")
                    result = extractor.parse_documents(file_path)
                    self.psql.create_tables()
                    ser_result = result[0].extraction
                    self.psql.insert_organization_person(ser_result, source=file_path, origin=origin, level=level)
            print("✅ business_data processed successfully.")
        except Exception as e:
            print(f"❌ Error in business_data: {e}")
            traceback.print_exc()

    def person_data(self, source_dir, origin="s3_bucket", level="2"):
        try:
            controller = StructuredDataController(
                files_dir=[source_dir],
                allowed_extensions=[".csv"],
                use_dask=False,
                level=level,
                origin=origin,
                skip_files=[]
            )
            processed_data = controller.process_files()
            all_columns = {col for df in processed_data.values() for col in df.columns}

            for _, df in processed_data.items():
                df_aligned = df.reindex(columns=all_columns).astype(str)
                self.psql.insert_df(df_aligned, "person_data", index=True)
            print("✅ person_data processed successfully.")
        except Exception as e:
            print(f"❌ Error in person_data: {e}")
            traceback.print_exc()

    # --------------------------------------------
    # Document Tasks
    # --------------------------------------------
    def run_document_task(self, task):
        print(f"\n=== Running document task: {task['name']} ===")
        try:
            if task.get("name") == "Product":
                print("Product task detected, skipping near search.")
                return

            processor = DocumentController(
                dir_path=task.get("dir_path"),
                level=task.get("level"),
                collection_name=task.get("collection_name"),
                properties=SCHEMA_MAP.get(task.get("properties")),
                collection_delete=task.get("collection_delete", False),
                product=task.get("product", False),
                origin=task.get("origin", None)
            )
            processor.run()

            if "retrieve_fields" in task and "retrieve_limit" in task:
                filters = None
                if "filters" in task:
                    f = task["filters"]
                    filters = DocumentController.Filter.by_property(f["field"]).equal(f["value"])
                processor.retrieve_data_by_field(
                    field_list=task["retrieve_fields"],
                    limit=task["retrieve_limit"],
                    filters=filters
                )

            if "hybrid_query" in task:
                print("Hybrid search\n", "*" * 50)
                processor.query_data_hybrid(
                    task["hybrid_query"],
                    limit=task.get("hybrid_limit", 10),
                    index_range=task.get("hybrid_index_range", 5)
                )
            print("✅ document task completed successfully.")
        except Exception as e:
            print(f"❌ Error in document task {task['name']}: {e}")
            traceback.print_exc()


# --------------------------------------------
# MAIN EXECUTION
# --------------------------------------------
if __name__ == "__main__":
    try:
        with open("tasks_config.json", "r", encoding="utf-8") as f:
            tasks = json.load(f)
    except Exception as e:
        print(f"❌ Failed to load tasks_config.json: {e}")
        sys.exit(1)

    structured_runner = StructuredRunner()


    # ✅ Run DB checks before processing
    pg_ok = structured_runner.check_postgres_connection()
    wv_ok = structured_runner.check_weaviate_connection()

    if not pg_ok and not wv_ok:
        print("❌ No database connections available. Aborting all tasks.")
        exit(1)

    if not pg_ok:
        print("⚠️ PostgreSQL connection failed. Structured data tasks will be skipped.")
    if not wv_ok:
        print("⚠️ Weaviate connection failed. Document tasks will be skipped.")

    # ✅ Continue only for available databases
    for task in tasks:
        task_type = task.get("type")
        task_name = task.get("name", "Unnamed Task")
        print(f"\n>>> Executing {task_type} task: {task_name} <<<")

        try:
            # --- Document tasks (require Weaviate) ---
            if task_type == "document":
                if not wv_ok:
                    print(f"⏭️ Skipping document task '{task_name}' — Weaviate not connected.")
                    continue
                structured_runner.run_document_task(task)

            # --- Structured tasks (require PostgreSQL) ---
            elif task_type == "structured":
                if not pg_ok:
                    print(f"⏭️ Skipping structured task '{task_name}' — PostgreSQL not connected.")
                    continue
                method_name = task.get("method")
                if hasattr(structured_runner, method_name):
                    getattr(structured_runner, method_name)(
                        source_dir=task.get("dir_path"),
                        origin=task.get("origin", "s3_bucket"),
                        level=task.get("level", "2")
                    )
                else:
                    print(f"⚠️ Unknown structured method: {method_name}")

            # --- Unknown task types ---
            else:
                print(f"⚠️ Unknown task type: {task_type}")

        except Exception as e:
            print(f"❌ Error executing task '{task_name}': {e}")
            traceback.print_exc()
