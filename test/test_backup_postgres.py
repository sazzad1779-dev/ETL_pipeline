import os
import subprocess
import boto3
import gzip
import tempfile
import logging
from datetime import datetime
from dotenv import load_dotenv

# ------------------ LOAD ENV ------------------
load_dotenv(override=True)

# ------------------ LOGGING ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)



class PostgresManager:
    def __init__(self, host_url: str):
        self.db_config = self.parse_pg_url(host_url)
        self.env = os.environ.copy()
        self.env["PGPASSWORD"] = self.db_config["password"]
        self.s3_bucket = os.getenv("S3_BUCKET")
        self.s3_region = os.getenv("S3_REGION")


    @staticmethod
    def parse_pg_url(host_url: str):
        return {
            "host": host_url,
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "database": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
        }

    def test_connection(self, name: str) -> bool:
        cmd = [
            "psql",
            "--host", self.db_config["host"],
            "--port", str(self.db_config["port"]),
            "--username", self.db_config["user"],
            "--dbname", self.db_config["database"],
            "--command", "SELECT version();",
        ]
        res = subprocess.run(cmd, env=self.env, capture_output=True, text=True)
        if res.returncode == 0:
            logger.info(f"✅ Connection OK to {name} ({self.db_config['host']}): {res.stdout.strip()}")
            return True
        logger.error(f"❌ Connection failed to {name} ({self.db_config['host']}): {res.stderr.strip()}")
        return False

    def backup_to_s3(self, s3_path: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{self.db_config['database']}_backup_{timestamp}.sql.gz"
        s3_key = f"{s3_path.rstrip('/')}/{backup_name}"
        s3_client = boto3.client("s3", region_name=self.s3_region)

        with tempfile.TemporaryDirectory() as tmp:
            local_path = os.path.join(tmp, backup_name)
            cmd = [
                "pg_dump",
                "--host", self.db_config["host"],
                "--port", str(self.db_config["port"]),
                "--username", self.db_config["user"],
                "--dbname", self.db_config["database"],
                "--no-password",
                "--clean", "--if-exists",
                "--no-owner", "--no-privileges",
                "--format=plain",
                "--encoding=UTF8",
                "--quote-all-identifiers",
            ]

            logger.info(f"Backing up {self.db_config['host']} → {backup_name}")
            proc = subprocess.run(cmd, env=self.env, capture_output=True, text=True)
            if proc.returncode != 0:
                raise RuntimeError(f"pg_dump failed: {proc.stderr}")

            # Compress output
            with gzip.open(local_path, "wt") as gz:
                gz.write(proc.stdout)

            s3_client.upload_file(local_path, self.s3_bucket, s3_key,
                                  ExtraArgs={"ServerSideEncryption": "AES256"})
            logger.info(f"✅ Backup uploaded to s3://{self.s3_bucket}/{s3_key}")
        return s3_key

    def restore_from_s3(self, s3_key: str):
        s3_client = boto3.client("s3", region_name=self.s3_region)

        with tempfile.TemporaryDirectory() as tmp:
            local_gz = os.path.join(tmp, "downloaded.sql.gz")
            local_sql = os.path.join(tmp, "restore.sql")

            logger.info(f"Downloading backup from S3: s3://{self.s3_bucket}/{s3_key}")
            s3_client.download_file(self.s3_bucket, s3_key, local_gz)

            logger.info("Decompressing backup...")
            with gzip.open(local_gz, "rt") as gz, open(local_sql, "w") as sql:
                sql.write(gz.read())

            cmd = [
                "psql",
                "--host", self.db_config["host"],
                "--port", str(self.db_config["port"]),
                "--username", self.db_config["user"],
                "--dbname", self.db_config["database"],
                "--no-password",
                "--single-transaction",
                "--set", "ON_ERROR_STOP=1",
                "--file", local_sql,
            ]

            logger.info(f"Restoring backup into {self.db_config['host']} ...")
            proc = subprocess.run(cmd, env=self.env, capture_output=True, text=True)
            if proc.returncode == 0:
                logger.info("✅ Restore completed successfully")
            else:
                raise RuntimeError(f"Restore failed: {proc.stderr}")


if __name__ == "__main__":
    PROD_HOST = os.getenv('PG_PROD_HOST')
    DEV_HOST = os.getenv('PG_DEV_HOST')
    LOCAL = "localhost"
    S3_DEV_PATH = os.path.join(os.getenv("S3_POSTGRES_PATH"), "dev")
    S3_PROD_PATH = os.path.join(os.getenv("S3_POSTGRES_PATH"), "prod")
    # Initialize managers
    local_manager = PostgresManager(LOCAL)
    prod_manager = PostgresManager(PROD_HOST)

    # 1️⃣ Verify connections
    if not local_manager.test_connection("LOCAL"):
        raise SystemExit("Cannot connect to LOCAL database.")
    if not prod_manager.test_connection("PROD"):
        raise SystemExit("Cannot connect to PROD database.")

    # 2️⃣ Backup PROD → S3
    s3_key = prod_manager.backup_to_s3( S3_DEV_PATH)

    # 3️⃣ Restore S3 → LOCAL
    local_manager.restore_from_s3(s3_key)
