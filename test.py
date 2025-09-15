import weaviate
from weaviate.auth import AuthApiKey
import os
from dotenv import load_dotenv
load_dotenv(override=True)
client = weaviate.Client(
    url=f"http://{os.getenv('WEAVIATE_URL')}:8080",
    auth_client_secret=AuthApiKey(os.getenv("WEAVIATE_API_KEY")),
    timeout_config=(5, 15)
)

if client.is_ready():
    print("✅ Weaviate is reachable and ready!")
else:
    print("❌ Weaviate is not ready or unreachable.")
