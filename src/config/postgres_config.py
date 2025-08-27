
import os
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
load_dotenv(override=True)
# Base class for all models
Base = declarative_base()

# Database URL (from env or fallback)
# PG_DB_URL = "postgresql+psycopg2://postgres:2244@localhost/xyz?client_encoding=utf8"

PG_DB_URL = os.environ.get(
    "PG_DB_URL",
    "postgresql+psycopg2://postgres:2244@localhost/sevensix_dev_1?client_encoding=utf8"
)
print("PG_DB_URL: ",PG_DB_URL)