from src.controller.weaviate_controller import WeaviateController
# from src
from dotenv import load_dotenv
load_dotenv()
weaviate_client = WeaviateController(
            collection_name="Product_data",
            embedding_provider="openai",
            #collection_delete=True
        )

print("\n\n")
# weaviate_client.retrieve_data_by_field(["input","response"], 125)
# import pandas as pd

# # Get the collection
# collection_name = weaviate_client._create_collection()
# # Fetch all objects including vectors
# data = []
# for item in collection_name.iterator(include_vector=False):
#     row = item.properties.copy()
#     # row['vector'] = item.vector  # Add the vector to the row
#     data.append(row)

# # Convert to pandas DataFrame and export
# df = pd.DataFrame(data)
# df.to_csv("product_data.csv", index=False)
# weaviate_client.query_data("に車輌の整備に心がけ、定期点検を怠", limit=1)
weaviate_client.client_close()

