import os
from src.utils.weaviate_backup import WeaviateBackupRestore
from src.utils.postgres_backup import PostgresManager
from dotenv import load_dotenv
load_dotenv(override=True)

DEV_HOST = os.getenv("DEV_HOST")
PROD_HOST = os.getenv("PROD_HOST")
LOCAL_HOST = "localhost"

def pg_backup_restore():
    S3_DEV_PATH = os.path.join(os.getenv("S3_POSTGRES_PATH"), "dev")
    S3_PROD_PATH = os.path.join(os.getenv("S3_POSTGRES_PATH"), "prod")
    # Initialize managers
    prod_manager = PostgresManager(PROD_HOST)
    local_manager = PostgresManager(LOCAL_HOST)

    # 1 Verify connections
    if not local_manager.test_connection("LOCAL"):
        raise SystemExit("Cannot connect to LOCAL database.")
    if not prod_manager.test_connection("PROD"):
        raise SystemExit("Cannot connect to PROD database.")

    # 2 Backup PROD → S3
    s3_key = prod_manager.backup_to_s3(S3_DEV_PATH)

    # 3 Restore S3 → LOCAL
    local_manager.restore_from_s3(s3_key)

def weaviate_backup_restore():
    DEV_PATH = os.path.join(os.getenv("S3_WEAVIATE_PATH"), "dev")
    PROD_PATH = os.path.join(os.getenv("S3_WEAVIATE_PATH"), "prod")

    # 1 Backup DEV
    dev_backup = WeaviateBackupRestore(DEV_HOST)
    backup_location, backup_id = dev_backup.create_backup(DEV_PATH)
    dev_backup.close()

    # 2 Restore to PROD (with automatic class deletion)
    prod_restore = WeaviateBackupRestore(LOCAL_HOST)
    prod_restore.restore_backup(backup_id, backup_location, delete_existing=True)
    prod_restore.close()
