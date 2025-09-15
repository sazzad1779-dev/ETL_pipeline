# run_tasks_from_json.py
import json
from dotenv import load_dotenv
load_dotenv(override=True)

from src.controller.document_controller import DocumentController
from src.schemas.weaviate import PRODUCT_SCHEMA, DEFAULT_SCHEMA

# Map schema names to actual objects
SCHEMA_MAP = {
    "PRODUCT_SCHEMA": PRODUCT_SCHEMA,
    "DEFAULT_SCHEMA": DEFAULT_SCHEMA
}

# Load tasks from JSON
with open("tasks_config.json", "r", encoding="utf-8") as f:
    tasks = json.load(f)

for task in tasks:
    print(f"\n=== Running task: {task['name']} ===")
    if task.get("name") == "Product" :
        print("Product task detected, skipping near search.")
        continue
    
    processor = DocumentController(
        dir_path=task.get("dir_path"),
        level=task.get("level"),
        collection_name=task.get("collection_name"),
        properties=SCHEMA_MAP.get(task.get("properties")),
        collection_delete=task.get("collection_delete", False),
        product=task.get("product", False),
        origin=task.get("origin", None)
    )
    
    processor.run()
    
    # Retrieve data
    if "retrieve_fields" in task and "retrieve_limit" in task:
        filters = None
        if "filters" in task:
            f = task["filters"]
            filters = DocumentController.Filter.by_property(f["field"]).equal(f["value"])
        processor.retrieve_data_by_field(
            field_list=task["retrieve_fields"],
            limit=task["retrieve_limit"],
            filters=filters
        )
    
    # Regular query
    # if "query" in task:
    #     print("Near search\n", "*"*50)
    #     processor.query_data(task["query"])
    
    # Hybrid query
    if "hybrid_query" in task:
        print("Hybrid search\n", "*"*50)
        processor.query_data_hybrid(
            task["hybrid_query"],
            limit=task.get("hybrid_limit", 10),
            index_range=task.get("hybrid_index_range", 5)
        )
