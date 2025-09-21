from weaviate.util import generate_uuid5
import weaviate
import json
import time
from datetime import datetime
from src.controller.document_controller import DocumentController
from src.schemas.weaviate import PRODUCT_SCHEMA
import weaviate.classes as wvc
from dotenv import load_dotenv
processor = DocumentController(
        level="1",
        collection_name="dummy_data",
        properties=PRODUCT_SCHEMA,
        collection_delete=False,
        product=True
    )
client = processor.weaviate_client.client
data_objects = []
for i in range(5):
    properties = {"question": f"Test Question {i+1}"}
    obj = wvc.data.DataObject(
            properties=properties,
            uuid=generate_uuid5(properties),  # deterministic UUID per object
            # vector=..., references=...  # optional
        )
    data_objects.append(obj)

response = client.collection.data.insert_many(data_objects)
print(response)