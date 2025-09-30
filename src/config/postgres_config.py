
import os
from sqlalchemy.orm import declarative_base
from dotenv import load_dotenv
load_dotenv(override=True)
# Base class for all models
Base = declarative_base()

if os.getenv("HOST_TYPE")=="local":
    PG_DB_URL = os.environ.get(
    "PG_DB_URL_LOCAL",None
)
elif os.getenv("HOST_TYPE")=="prod":
    PG_DB_URL = os.environ.get(
    "PG_DB_URL_DEV",None
)
elif os.getenv("HOST_TYPE")=="dev":
    PG_DB_URL = os.environ.get(
    "PG_DB_URL_DEV",None
)
print("PG_DB_URL: ",PG_DB_URL)