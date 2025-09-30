from src.utils.weaviate_monitoring import weaviate_monitor
from src.utils.postgres_monitoring import monitor_public_tables
from dotenv import load_dotenv
load_dotenv(override=True)

def weaviate_monitoring():
    weaviate_monitor("dev")
def postgres_monitoring():
    monitor_public_tables("dev") #prod #local

# postgres_monitoring()
weaviate_monitoring()