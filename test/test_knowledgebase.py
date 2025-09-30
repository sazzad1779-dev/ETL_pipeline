import os
import weaviate
from weaviate.classes.init import Auth
from weaviate.classes.query import Filter
from dotenv import load_dotenv
load_dotenv(override=True)

def _connect(embedding_provider):
    headers = {}
    weaviate_api_key = os.environ["WEAVIATE_API_KEY"]
    if embedding_provider == "jina":
        headers["X-JinaAI-Api-Key"] = os.getenv("JINAAI_API_KEY")
    elif embedding_provider == "openai":
        headers["X-OpenAI-Api-Key"] = os.getenv("OPENAI_API_KEY")
    host_type = os.getenv("HOST_TYPE")
    if host_type == "local":
        print("Connecting to local Weaviate instance...")
        client = weaviate.connect_to_local(
            headers=headers,
            auth_credentials=Auth.api_key(weaviate_api_key)
        )
    elif host_type == "prod":
        print(f"Connecting to Weaviate at {os.getenv('PROD_HOST')}...")
        client = weaviate.connect_to_custom(
            headers=headers,
            http_host=os.getenv("PROD_HOST"),
            http_port=8080,
            http_secure=False,
            grpc_host=os.getenv("PROD_HOST"),
            grpc_port=50051,
            auth_credentials=Auth.api_key(weaviate_api_key),
            skip_init_checks=True,
            grpc_secure=False,
        )
    elif host_type == "dev":
        print(f"Connecting to Weaviate at {os.getenv('DEV_HOST')}...")
        client = weaviate.connect_to_custom(
            headers=headers,
            http_host=os.getenv("DEV_HOST"),
            http_port=8080,
            http_secure=False,
            grpc_host=os.getenv("DEV_HOST"),
            grpc_port=50051,
            auth_credentials=Auth.api_key(weaviate_api_key),
            skip_init_checks=True,
            grpc_secure=False,
        )
    else:
        raise ValueError("Unknown HOST_TYPE")
    if client.is_ready():
        print("Connected to Weaviate")
    else:
        print("Connection failed")
    return client

def fetch_and_save_chunks(embedding_provider, collection_name="Product_data", output_file="chunk_index_0.txt"):
    client = _connect(embedding_provider)
    chunks = client.collections.use(collection_name)
    # Use the Filter class as required by the v4 client
    filters = Filter.by_property("chunk_index").equal(0)
    response = chunks.query.fetch_objects(
        filters=filters,
        return_properties=["content", "source", "level", "image_urls", "youtube_urls", "chunk_index"],
        limit=1000  # Adjust as needed
    )
    with open(output_file, "w", encoding="utf-8") as f:
        for obj in response.objects:
            f.write("Product "+obj.properties["content"] + "\n\n")
    print(f"Saved {len(response.objects)} chunks to {output_file}")
    client.close()

# Example usage:
fetch_and_save_chunks("openai")