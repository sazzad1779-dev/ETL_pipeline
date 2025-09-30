import os
import time
from datetime import datetime
import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.backup import BackupLocation, BackupStorage
import logging

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class WeaviateBackupRestore:
    def __init__(self, host: str = None, secure: bool = False):
        self.headers = {"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")}
        self.host = host or os.getenv("DEV_HOST")

        self.client = weaviate.connect_to_custom(
            headers=self.headers,
            http_host=self.host,
            http_port=int(os.getenv("http_port", 8080)),
            http_secure=secure,
            grpc_host=self.host,
            grpc_port=int(os.getenv("grpc_port", 50051)),
            grpc_secure=secure,
            auth_credentials=Auth.api_key(os.getenv("WEAVIATE_API_KEY", "jbc_admin")),
            skip_init_checks=True,
        )

    def close(self):
        self.client.close()
        logger.info("Closed Weaviate connection to %s", self.host)

    def delete_classes(self, class_names: list[str]):
        """Delete specified classes if they exist"""
        for class_name in class_names:
            try:
                if self.client.collections.exists(class_name):
                    self.client.collections.delete(class_name)
                    logger.info("Deleted class: %s", class_name)
                else:
                    logger.info("Class not found (skipped): %s", class_name)
            except Exception as e:
                logger.warning("Could not delete class %s: %s", class_name, e)

    def delete_all_classes(self):
        """Delete all classes from the Weaviate instance"""
        try:
            collections = self.client.collections.list_all()
            for collection_name in collections:
                self.client.collections.delete(collection_name)
                logger.info("Deleted class: %s", collection_name)
        except Exception as e:
            logger.warning("Could not delete all classes: %s", e)

    def create_backup(self, s3_path: str, wait: bool = True):
        backup_id = f"weaviate_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_location = BackupLocation.S3(
            bucket=os.getenv("S3_BUCKET"),
            path=s3_path,
            region=os.getenv("S3_REGION")
        )
        logger.info("Starting backup in %s with ID: %s", self.host, backup_id)
        self.client.backup.create(
            backend=BackupStorage.S3,
            backup_id=backup_id,
            backup_location=backup_location
        )
        if wait:
            self._wait_for_status("create", backup_id, backup_location)
        return backup_location, backup_id

    def restore_backup(
        self,
        backup_id: str,
        backup_location: BackupLocation,
        wait: bool = True,
        delete_existing: bool = True
    ):
        if delete_existing:
            logger.info("Deleting existing classes in %s...", self.host)
            self.delete_all_classes()

        logger.info("Starting restore in %s with ID: %s", self.host, backup_id)
        self.client.backup.restore(
            backend=BackupStorage.S3,
            backup_id=backup_id,
            backup_location=backup_location,
            wait_for_completion=True
        )
        if wait:
            self._wait_for_status("restore", backup_id, backup_location)

    def _wait_for_status(
        self,
        action: str,
        backup_id: str,
        backup_location: BackupLocation,
        interval: int = 5
    ):
        status_func = {
            "create": self.client.backup.get_create_status,
            "restore": self.client.backup.get_restore_status
        }.get(action)
        if not status_func:
            raise ValueError("Invalid action for status check")

        while True:
            status = status_func(
                backend=BackupStorage.S3,
                backup_id=backup_id,
                backup_location=backup_location
            )
            logger.info("%s status: %s", action.capitalize(), status.status)
            if status.status in ("SUCCESS", "FAILED"):
                break
            time.sleep(interval)

        if status.status != "SUCCESS":
            logger.error("%s failed: %s", action.capitalize(), status.error)
            raise Exception(f"{action.capitalize()} failed: {status.error}")
        logger.info("%s completed successfully in %s.", action.capitalize(), self.host)
