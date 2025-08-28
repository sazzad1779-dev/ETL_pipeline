from weaviate.classes.query import MetadataQuery, Filter
import json
class WeaviateUtils:
    def __init__(self, collection):
        self.collection = collection

    def insert_data(self, **kwargs):
        """Insert a single object into the collection."""
        try:
            lengths = {len(v) for v in kwargs.values()}
            if len(lengths) != 1:
                raise ValueError("All property lists must have the same length.")
            data = [dict(zip(kwargs.keys(), vals)) for vals in zip(*kwargs.values())]
            print(f"📥 Inserting {len(data)} items...")
            response = self.collection.data.insert_many(data)
            if response.has_errors:
                print("❌ Insert Errors:", response.errors)
            else:
                print("✅ Insert complete.")
        except  Exception as e :
            print(f"❌ Error: {e}")
            return {"error": str(e)}
            


    def run_query(self, query_text, limit=5):
        """Execute a near_text query and print results."""
        print(f"\n🔍 Querying for: {query_text}\n")
        response = self.collection.query.near_text(
            query=query_text,
            limit=limit,
            return_metadata=MetadataQuery(distance=True)
        )
        for i, obj in enumerate(response.objects, start=1):
            print(f"Result #{i}:")
            for prop in obj.properties:
                print(f"{prop}:", obj.properties.get(prop))
            print("Distance:", obj.metadata.distance)
            print("---")

    def run_query_hybrid(self, query_text, limit=5):
        """Execute a near_text query and print results."""
        print(f"\n🔍 Querying for: {query_text}\n")
        response = self.collection.query.hybrid(
            query=query_text,
            limit=limit,
            alpha=0.3,
            return_properties=["content", "source"],
            return_metadata=MetadataQuery(distance=True),
        )
        for i, obj in enumerate(response.objects, start=1):
            print(f"Result #{i}:")
            for prop in obj.properties:
                print(f"{prop}:", obj.properties.get(prop))
            print("Distance:", obj.metadata.distance)
            print("---")

    def delete_by_source(self, file_source: str):
        """Delete objects by 'source' property."""
        print(f"🗑️ Deleting objects with source: {file_source}")
        result = self.collection.data.delete_many(
            where=Filter.by_property("source").equal(file_source)
        )
        print(f"Deleted {result.matches} objects, failed {result.failed}")

    def retrieve_by_field(self, field_list, limit=5):
        """Retrieve data using a near_text query."""
        limit = limit - 1
        for item in self.collection.iterator(include_vector=True):
            for field in field_list:
                print(f"{field}:", json.dumps(item.properties.get(field), indent=2, ensure_ascii=False))
            print("---")
            if limit == 0:
                break
            limit -= 1

    def print_collection_info(self):
        """Print collection metadata."""
        print(f"Collection Name: {self.collection.name}")
        print(f"Tenancy: {self.collection.tenants.get()}")
        return self.collection
    