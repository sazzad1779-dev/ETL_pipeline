from weaviate.classes.query import MetadataQuery, Filter
import json
from typing import Dict
from weaviate.classes.query import HybridFusion
from pydantic import BaseModel, Field
from typing import List, Optional
import weaviate.classes as wvc
from weaviate.util import generate_uuid5  # Generate a deterministic ID
class Source(BaseModel):
    """Source information for guideline responses"""

    name: str = Field(..., description="Name or identifier of the source")
    url: Optional[str] = Field(None, description="URL of the source if available")
    score: Optional[float] = Field(
        None, description="Relevance score from vector search"
    )

class WeaviateUtils:
    def __init__(self, collection):
        self.collection = collection

    # def insert_data(self, **kwargs):
    #     """Insert a single object into the collection."""
    #     try:
    #         lengths = {len(v) for v in kwargs.values()}
    #         if len(lengths) != 1:
    #             raise ValueError("All property lists must have the same length.")
    #         data = [dict(zip(kwargs.keys(), vals)) for vals in zip(*kwargs.values())]
    #         print(f"📥 Inserting {len(data)} items...")
    #         response = self.collection.data.insert_many(data)
    #         if response.has_errors:
    #             print("❌ Insert Errors:", response.errors)
    #         else:
    #             print("✅ Insert complete.")
    #     except  Exception as e :
    #         print(f"❌ Error: {e}")
    #         return {"error": str(e)}
    def insert_data(self, **kwargs):
        """Insert a list of objects where each object = {'properties': {...}, 'uuid': ...}."""
        try:
            # Validate lengths of remaining fields
            lengths = {len(v) for v in kwargs.values()}
            if len(lengths) != 1:
                raise ValueError("All property lists must have the same length.")
            data_objects = []
            for i, vals in enumerate(zip(*kwargs.values())):
                # Build the properties dict from each row of values
                properties = dict(zip(kwargs.keys(), vals))
                # Create the object with the UUID
                obj = wvc.data.DataObject(
                    properties=properties,
                    uuid=generate_uuid5(properties),  
                )
                data_objects.append(obj)
            print(f"📥 Inserting {len(data_objects)} items...")
            response = self.collection.data.insert_many(data_objects)

            if response.has_errors:
                print("❌ Insert Errors:", response.errors)
            else:
                print("✅ Insert complete.")
        except Exception as e:
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

    def run_query_single_chunk_hybrid(self, query_text, limit=5):
        """Execute a near_text query and print results."""
        print(f"\n🔍 Querying for: {query_text}\n")

        response = self.collection.query.hybrid(
            query=query_text,
            limit=limit,
            alpha=0.5,
            return_properties=["content", "source"],
            return_metadata=MetadataQuery(distance=True,score=True,certainty=True),
            fusion_type = HybridFusion.RELATIVE_SCORE,
            auto_limit=2,
        )
        source_set = set()

        for i, obj in enumerate(response.objects, start=1):
            print(f"Result #{i}:")
            if obj.properties.get("source") in source_set:
                print("Duplicate source found, skipping...")
                continue
            source_set.add(obj.properties.get("source"))
            for prop in obj.properties:
                print(f"{prop}:", obj.properties.get(prop))
            print("Distance:", obj.metadata.score)
            print("---")
            # relative= self.collection.query.fetch_objects(return_properties=["content", "source"],filter=Filter.by_property("source").equal(obj.properties["source"]))
            relative = self.collection.query.fetch_objects(
                return_properties=["content"],
                filters=Filter.by_property("source").equal(obj.properties["source"]),
                limit=10,  # optional
            )
            print(" Related items ")
            print("===-+++==="*20)
            total_content= len(relative.objects)
            print(f"Total related items: {total_content}")
            for obj1 in relative.objects:
                for prop1 in obj1.properties:
                    print(f"{prop1}:", obj1.properties.get(prop1))
            print("===+==="*20)
 
    def search_by_source(self, source_set=None, index_set=None, index_range=2, limit=5):
        """Search objects by 'source' and index range (single query)."""

        source_set = source_set or []
        index_set = index_set or []
        all_objects = []
        sources_list = []
        results = []
        print(f"source_set: {source_set}, index_set: {index_set}, range: {index_range}")
        if not source_set or not index_set:
            print("❌ No sources or indexes provided for search.")
            return
        contents = ""
        for source, index in zip(source_set, index_set):
            start_index = max(index - index_range, 0)
            end_index = index + index_range

            print(f"\n🔍 Searching for source: {source}, range: {start_index} → {end_index}")
            filters = (
                        Filter.by_property("source").equal(source)
                        & Filter.by_property("chunk_index").greater_or_equal(start_index)
                        & Filter.by_property("chunk_index").less_or_equal(end_index)
                    )
            # Always fetch the common fields
            base_props = ["content", "source", "chunk_index"]

            # Add optional fields only if they exist in the schema
            optional_props = []
            schema_props = [p.name for p in self.collection.config.get().properties]

            if "image_urls" in schema_props:
                optional_props.append("image_urls")
            if "youtube_urls" in schema_props:
                optional_props.append("youtube_urls")

            response = self.collection.query.fetch_objects(
                return_properties=base_props + optional_props,
                filters=filters,
                limit=limit,
            )
            print(f"Filter: source={source}, chunk_index in [{start_index}, {end_index}]")
            print(f"Length of response objects: {len(response.objects)}")
            if not response.objects:
                print(f"⚠️ No chunks found for {source} in range {start_index} → {end_index}")
                continue

            print(f"Retrieved {len(response.objects)} chunks:")
            for obj in sorted(response.objects, key=lambda x: x.properties["chunk_index"]):
                print(f"  [{obj.properties['chunk_index']}] {obj.properties['content']}")
                contents += f"{obj.properties['content']}\n"
            contents += "\n"
            # contents += self.extract_sources(response.objects[0])
            score=.5
            obj = response.objects[0]
            source = obj.properties.get("source", "Unknown Product")
            if obj.properties.get("image_urls", None) or obj.properties.get("youtube_urls", None):
                image_urls = (
                    obj.properties.get("image_urls", "").split("\n")
                    if obj.properties.get("image_urls")
                    else []
                )
                youtube_urls = (
                    obj.properties.get("youtube_urls", "").split("\n")
                    if obj.properties.get("youtube_urls")
                    else []
                )
                # Clean up URLs
                image_urls = [url.strip() for url in image_urls if url.strip()]
                youtube_urls = [url.strip() for url in youtube_urls if url.strip()]
                # Add image sources

                for i, img_url in enumerate(image_urls[:3]):
                    sources_list.append(
                        Source(name=f"Product Image {i + 1}", url=img_url, score=None)
                    )

                # Add video sources
                for i, video_url in enumerate(youtube_urls[:2]):
                    sources_list.append(
                        Source(name=f"Product Video {i + 1}", url=video_url, score=None)
                    )
            
            # Add main product source
            if source.startswith(("http://", "https://")):
                sources_list.append(
                    Source(
                        name=f"Product: {source.split('/')[-1] if '/' in source else source}",
                        url=source,
                        score=score,
                    )
                )
            results.append(contents)
            
        formatted_content = "\n\n" + ("\n" + "=" * 50 + "\n\n").join(results)
        print({"content": formatted_content, "sources": sources_list})
        


    def run_query_hybrid(self, query_text, limit=5,index_range=50):
        """Execute a near_text query and print results."""
        print(f"\n🔍 Querying for: {query_text}\n")
        response = self.collection.query.hybrid(
            query=query_text,
            limit=limit,
            alpha=0.7,
            return_properties=["content", "source","chunk_index"],
            return_metadata=MetadataQuery(score=True),
            fusion_type = HybridFusion.RELATIVE_SCORE,
            auto_limit=2,
        )
        source_set ,index_set= [],[]
        for i, obj in enumerate(response.objects, start=1):
            if obj.properties.get("source") in source_set:
                continue
            source_set.append(obj.properties.get("source"))
            index_set.append(obj.properties.get("chunk_index"))

        print(" Related items ")
        print("* * * * *"*20)
        self.search_by_source(source_set,index_set,index_range=index_range, limit=limit)
        print("* * * * *"*20)

    def delete_by_source(self, file_source: str):
        """Delete objects by 'source' property."""
        print(f"🗑️ Deleting objects with source: {file_source}")
        result = self.collection.data.delete_many(
            where=Filter.by_property("source").equal(file_source)
        )
        print(f"Deleted {result.matches} objects, failed {result.failed}")

    # def retrieve_by_field(self, field_list, limit=5,filters=None):
    #     """Retrieve data using a near_text query."""
    #     limit = limit - 1
    #     for item in self.collection.iterator(include_vector=True):
    #         for field in field_list:
    #             print(f"{field}:", json.dumps(item.properties.get(field), indent=2, ensure_ascii=False))
    #         print("---")
    #         if limit == 0:
    #             break
    #         limit -= 1

    def retrieve_by_field(self, field_list, limit=5, filters=None):
        """ Retrieve chunks based """
        resp = self.collection.query.fetch_objects(
            return_properties=field_list,
            limit=limit,
            filters=filters,  # e.g., Filter.by_property("source").equal("my_source")
            include_vector=True
        )
        for item in resp.objects:
            for field in field_list:
                print(f"{field}:", json.dumps(item.properties.get(field), indent=2, ensure_ascii=False))
            print("---")
        return resp.objects
    
    def delete_data_by_source_list(self, file_sources: List[str]):
        """Delete objects by a list of 'source' properties."""
        print(f"🗑️ Deleting objects with sources: {file_sources}")
        for file_source in file_sources:
            result = self.collection.data.delete_many(
                where=Filter.by_property("source").equal(file_source)
            )
            print(f"Deleted {result.matches} objects for source '{file_source}', failed {result.failed}")

    
    def print_collection_info(self):
        """Print collection metadata."""
        print(f"Collection Name: {self.collection.name}")
        print(f"Tenancy: {self.collection.tenants.get()}")
        return self.collection
    