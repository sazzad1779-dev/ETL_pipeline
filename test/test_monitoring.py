from src.utils.weaviate_monitoring import weaviate_monitor
from src.utils.postgres_monitoring import monitor_public_tables

from weaviate.classes.init import Auth
from dotenv import load_dotenv
import os
import weaviate
load_dotenv(override=True)
# Replace with your actual prod API key

def weaviate_monitoring():
    weaviate_monitor("dev")
def postgres_monitoring():
    monitor_public_tables("local") #prod #local

postgres_monitoring()
# weaviate_monitoring()