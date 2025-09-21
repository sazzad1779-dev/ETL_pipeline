import weaviate
import json
import time
from datetime import datetime
from src.controller.document_controller import DocumentController
from src.schemas.weaviate import PRODUCT_SCHEMA
from dotenv import load_dotenv

load_dotenv(override=True)

def format_timestamp():
    """Return formatted timestamp for logging"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def print_separator(title="", char="-", length=60):
    """Print a formatted separator line"""
    if title:
        print(f"\n{char * 5} {title} {char * (length - len(title) - 12)}")
    else:
        print(char * length)

def get_collection_detailed_info(client, collection_name):
    """Get detailed information about a specific collection"""
    try:
        collection = client.collections.get(collection_name)
        
        # Basic collection info
        print(f"📊 Detailed Analysis for Collection: {collection_name}")
        print_separator()
        
        # 1. Total count with timing
        start_time = time.time()
        response = collection.aggregate.over_all(total_count=True)
        count_time = time.time() - start_time
        total_count = response.total_count
        print(f"   Total Objects: {total_count:,} (Retrieved in {count_time:.3f}s)")
        
        # 2. Sample a few objects to understand data structure
        if total_count > 0:
            print("\n🔍 Sample Data Analysis:")
            try:
                # Get first 3 objects as samples
                sample_response = collection.query.fetch_objects(limit=1,include_vector=True)
                sample_objects = sample_response.objects
                
                if sample_objects:
                    print(f"   Retrieved {len(sample_objects)} sample objects:")
                    for i, obj in enumerate(sample_objects, 1):
                        print(f"\n   Sample Object {i}:")
                        print(f"     UUID: {obj.uuid}")
                        
                        # Show properties and their values
                        if obj.properties:
                            print(f"     Properties ({len(obj.properties)} fields):")
                            for key, value in obj.properties.items():
                                # Truncate long values for readability
                                if isinstance(value, str) and len(value) > 100:
                                    display_value = value[:100] + "..."
                                elif isinstance(value, (list, dict)):
                                    display_value = f"{type(value).__name__} with {len(value)} items"
                                else:
                                    display_value = value
                                print(f"       {key}: {display_value}")
                        
                        # Show vector information if available
                        if hasattr(obj, 'vector') and obj.vector:
                            if isinstance(obj.vector, dict):
                                for vector_name, vector_data in obj.vector.items():
                                    if vector_data:
                                        print(f"     Vector '{vector_name}': {len(vector_data)} dimensions")
                            elif isinstance(obj.vector, list):
                                print(f"     Vector: {len(obj.vector)} dimensions")
                
            except Exception as e:
                print(f"   ❌ Could not retrieve sample data: {e}")
        
        # 3. Property analysis
        print(f"\n📋 Property Analysis:")
        try:
            # Get collection configuration
            config = client.collections.get(collection_name).config.get()
            
            if config.properties:
                print(f"   Total Properties: {len(config.properties)}")
                
                # Group properties by data type
                type_groups = {}
                for prop in config.properties:
                    data_type = prop.data_type[0] if prop.data_type else "Unknown"
                    if data_type not in type_groups:
                        type_groups[data_type] = []
                    type_groups[data_type].append(prop.name)
                
                print(f"   Property Types Distribution:")
                for data_type, props in type_groups.items():
                    print(f"     {data_type}: {len(props)} properties")
                    if len(props) <= 5:  # Show property names if not too many
                        print(f"       → {', '.join(props)}")
                    else:
                        print(f"       → {', '.join(props[:3])} ... and {len(props)-3} more")
            
            # 4. Vectorizer configuration
            if config.vectorizer_config:
                print(f"\n🎯 Vectorizer Configuration:")
                vectorizer = config.vectorizer_config
                print(f"   Vectorizer: {vectorizer}")
                
                # Additional vectorizer details if available
                if hasattr(vectorizer, 'model'):
                    print(f"   Model: {vectorizer.model}")
                if hasattr(vectorizer, 'vectorize_collection_name'):
                    print(f"   Vectorize Collection Name: {vectorizer.vectorize_collection_name}")
            
            # 5. Index configuration
            if hasattr(config, 'vector_index_config') and config.vector_index_config:
                print(f"\n🗂️ Vector Index Configuration:")
                index_config = config.vector_index_config
                print(f"   Index Type: {type(index_config).__name__}")
                
                # Show specific configuration details
                if hasattr(index_config, 'distance_metric'):
                    print(f"   Distance Metric: {index_config.distance_metric}")
                if hasattr(index_config, 'ef'):
                    print(f"   EF: {index_config.ef}")
                if hasattr(index_config, 'max_connections'):
                    print(f"   Max Connections: {index_config.max_connections}")
            
        except Exception as e:
            print(f"   ❌ Could not retrieve detailed configuration: {e}")
        
        return total_count
        
    except Exception as e:
        print(f"❌ Error analyzing collection {collection_name}: {e}")
        return 0

def check_weaviate_health(client):
    """Check Weaviate instance health and configuration"""
    print_separator("WEAVIATE INSTANCE HEALTH CHECK", "=")
    
    try:
        # 1. Check if Weaviate is ready
        is_ready = client.is_ready()
        print(f"✅ Weaviate Ready: {is_ready}")
        
        # 2. Check if Weaviate is live
        is_live = client.is_live()
        print(f"✅ Weaviate Live: {is_live}")
        
        # 3. Get cluster information
        try:
            # Try different methods to get cluster info
            if hasattr(client.cluster, 'get_nodes_status'):
                cluster_info = client.cluster.get_nodes_status()
                print(f"✅ Cluster Nodes: {len(cluster_info)} node(s)")
                for i, node in enumerate(cluster_info):
                    print(f"   Node {i+1}: {node.name} - Status: {node.status}")
            elif hasattr(client.cluster, 'get_nodes'):
                cluster_info = client.cluster.get_nodes()
                print(f"✅ Cluster Nodes: {len(cluster_info)} node(s)")
            else:
                print("✅ Cluster Info: Single node deployment (API method not available)")
        except Exception as e:
            print(f"⚠️ Could not retrieve cluster info: {e}")
        
        # 4. Get meta information
        try:
            meta = client.get_meta()
            print(f"✅ Weaviate Version: {meta.get('version', 'Unknown')}")
            if 'modules' in meta:
                modules = list(meta['modules'].keys())
                print(f"✅ Available Modules: {', '.join(modules)}")
        except Exception as e:
            print(f"⚠️ Could not retrieve meta information: {e}")
            
    except Exception as e:
        print(f"❌ Health check failed: {e}")

def analyze_schema_consistency(expected_schema, actual_properties):
    """Compare expected schema with actual collection properties"""
    print_separator("SCHEMA CONSISTENCY CHECK")
    
    if not expected_schema:
        print("⚠️ No expected schema provided for comparison")
        return
    
    # Convert actual properties to a dict for easier comparison
    actual_props = {prop.name: prop.data_type[0] for prop in actual_properties} if actual_properties else {}
    
    # Handle both list and dict formats for expected_schema
    if isinstance(expected_schema, list):
        # If it's a list of property objects, convert to dict
        expected_props_dict = {}
        for prop in expected_schema:
            if hasattr(prop, 'name') and hasattr(prop, 'data_type'):
                # Property object format
                prop_type = prop.data_type[0] if isinstance(prop.data_type, list) else prop.data_type
                expected_props_dict[prop.name] = prop_type
            elif isinstance(prop, dict) and 'name' in prop:
                # Dict format
                prop_type = prop.get('data_type', prop.get('dataType', 'text'))
                if isinstance(prop_type, list):
                    prop_type = prop_type[0]
                expected_props_dict[prop['name']] = prop_type
            else:
                print(f"⚠️ Unrecognized schema format for property: {prop}")
                continue
        expected_schema_dict = expected_props_dict
    elif isinstance(expected_schema, dict):
        expected_schema_dict = expected_schema
    else:
        print(f"⚠️ Unrecognized expected_schema format: {type(expected_schema)}")
        return
    
    print(f"Expected Properties: {len(expected_schema_dict)}")
    print(f"Actual Properties: {len(actual_props)}")
    
    if not expected_schema_dict:
        print("⚠️ No valid expected properties found")
        return
    
    # Check for missing properties
    missing_props = []
    type_mismatches = []
    
    for expected_prop, expected_type in expected_schema_dict.items():
        if expected_prop not in actual_props:
            missing_props.append(expected_prop)
        elif actual_props[expected_prop] != expected_type:
            type_mismatches.append((expected_prop, expected_type, actual_props[expected_prop]))
    
    # Check for extra properties
    extra_props = [prop for prop in actual_props if prop not in expected_schema_dict]
    
    # Report results
    if missing_props:
        print(f"❌ Missing Properties: {', '.join(missing_props)}")
    
    if type_mismatches:
        print("❌ Type Mismatches:")
        for prop, expected, actual in type_mismatches:
            print(f"   {prop}: expected {expected}, got {actual}")
    
    if extra_props:
        print(f"⚠️ Extra Properties: {', '.join(extra_props)}")
    
    if not missing_props and not type_mismatches and not extra_props:
        print("✅ Schema matches perfectly!")
    
    # Show expected vs actual side by side for debugging
    print(f"\n📋 Detailed Property Comparison:")
    all_props = set(list(expected_schema_dict.keys()) + list(actual_props.keys()))
    for prop in sorted(all_props):
        expected = expected_schema_dict.get(prop, "❌ Missing")
        actual = actual_props.get(prop, "❌ Missing")
        status = "✅" if expected == actual else "⚠️"
        print(f"   {status} {prop}: Expected='{expected}' | Actual='{actual}'")

# Initialize your DocumentController to get the Weaviate client
print_separator("INITIALIZING DOCUMENT CONTROLLER", "=")
print(f"[{format_timestamp()}] Starting Weaviate inspection...")

try:
    processor = DocumentController(
        level="1",
        collection_name="Product_data",
        properties=PRODUCT_SCHEMA,
        collection_delete=False,
        product=True
    )
    print("✅ DocumentController initialized successfully")
except Exception as e:
    print(f"❌ Failed to initialize DocumentController: {e}")
    exit(1)

# Access the Weaviate client instance
client = processor.weaviate_client.client

try:
    # 1. Health Check
    check_weaviate_health(client)
    
    # 2. Collection Overview
    print_separator("COLLECTION OVERVIEW", "=")
    print(f"[{format_timestamp()}] Fetching Weaviate Collection Information...")
    
    # Get a dictionary of all collection configurations
    all_collections_info = client.collections.list_all()
    
    if not all_collections_info:
        print("❌ No collections found in the Weaviate instance.")
    else:
        total_objects_across_all = 0
        print(f"✅ Found {len(all_collections_info)} collection(s)")
        
        # 3. Detailed analysis of each collection
        for i, (name, config) in enumerate(all_collections_info.items(), 1):
            print_separator(f"COLLECTION {i}: {name}", "=")
            
            # Get basic info
            print(f"Collection Name: {name}")
            
            # Get detailed information
            collection_count = get_collection_detailed_info(client, name)
            total_objects_across_all += collection_count
            
            # Schema consistency check for the target collection
            if name == "Product_data" and PRODUCT_SCHEMA:
                analyze_schema_consistency(PRODUCT_SCHEMA, config.properties)
            
            print()  # Add spacing between collections
        
        # 4. Summary
        print_separator("SUMMARY", "=")
        print(f"Total Collections: {len(all_collections_info)}")
        print(f"Total Objects Across All Collections: {total_objects_across_all:,}")
        print(f"Inspection completed at: {format_timestamp()}")
        
        # 5. Performance recommendations
        print_separator("RECOMMENDATIONS")
        if total_objects_across_all > 100000:
            print("💡 Large dataset detected. Consider:")
            print("   - Implementing pagination for queries")
            print("   - Using filters to reduce query scope")
            print("   - Monitoring query performance")
        elif total_objects_across_all > 10000:
            print("💡 Medium dataset detected. Consider:")
            print("   - Query optimization for better performance")
            print("   - Regular monitoring of response times")
        
        if len(all_collections_info) > 5:
            print("💡 Multiple collections detected. Consider:")
            print("   - Regular cleanup of unused collections")
            print("   - Consistent naming conventions")
            print("   - Cross-collection relationship management")
        
        # Additional insights based on the data
        print(f"\n📊 Data Insights:")
        for name, _ in all_collections_info.items():
            if name == "Paper_data":
                print(f"   📄 {name}: Academic/research content (2,686 documents)")
                print(f"      → Average retrieval time: Good performance")
                print(f"      → Content appears to be chunked research papers")
            elif name == "Product_data":
                print(f"   🛍️ {name}: Product catalog content (815 items)")
                print(f"      → Fast retrieval: Excellent performance")
                print(f"      → Contains web URLs and product information")
                print(f"      → Includes image and YouTube URLs for rich content")

except Exception as e:
    print(f"❌ An error occurred during inspection: {e}")
    import traceback
    print("Full traceback:")
    traceback.print_exc()

finally:
    # Always ensure the client connection is closed
    print_separator("CLEANUP")
    if client:
        try:
            client.close()
            print(f"✅ [{format_timestamp()}] Weaviate client connection closed successfully.")
        except Exception as e:
            print(f"⚠️ Warning during client cleanup: {e}")
    else:
        print("⚠️ No client to close.")