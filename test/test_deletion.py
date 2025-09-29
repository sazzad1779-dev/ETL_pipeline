import os
from dotenv import load_dotenv
import weaviate
from weaviate.classes.init import Auth

# Load environment variables
load_dotenv(override=True)

PROD_URL = "http://sevensix-prod-etl-nlb-wvpg-38d312dc5dbd8904.elb.ap-northeast-1.amazonaws.com/"
PROD_API_KEY = "jbc_admin"   # Replace with your actual prod API key

headers = {
    "X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")
}

# Connect to Weaviate
prod_client = weaviate.connect_to_custom(
    headers=headers,
    http_host=PROD_URL.replace("http://", "").replace("https://", "").rstrip("/"),
    http_port=8080,
    http_secure=False,
    grpc_host=PROD_URL.replace("http://", "").replace("https://", "").rstrip("/"),
    grpc_port=50051,
    grpc_secure=False,
    auth_credentials=Auth.api_key(PROD_API_KEY),
    skip_init_checks=True,
)

# --- Option 1: Delete all collections at once ---
prod_client.collections.delete_all()
print("All collections deleted.")

# --- Option 2: Delete collections one by one ---
# collections = prod_client.collections.list_all(simple=True)
# for collection_name in collections.keys():
#     prod_client.collections.delete(collection_name)
#     print(f"Deleted collection: {collection_name}")

prod_client.close()