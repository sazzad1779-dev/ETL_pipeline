import weaviate
import json
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
# from src.controller.document_controller import DocumentController
from src.schemas.weaviate import PRODUCT_SCHEMA
from dotenv import load_dotenv
import os
from weaviate.classes.init import Auth
class WeaviateInspector:
    """A comprehensive Weaviate collection inspector"""
    
    def __init__(self, host: str = None, secure: bool = False):
        """Initialize with a DocumentController instance"""
        self.headers = {"X-OpenAI-Api-Key": os.getenv("OPENAI_API_KEY")}
        self.host = host or os.getenv("DEV_HOST")
        self.client = weaviate.connect_to_custom(
            headers=self.headers,
            http_host=self.host,
            http_port=int(os.getenv("http_port", 8080)),
            http_secure=secure,
            grpc_host=self.host,
            grpc_port=int(os.getenv("grpc_port", 50051)),
            grpc_secure=secure,
            auth_credentials=Auth.api_key(os.getenv("WEAVIATE_API_KEY", "jbc_admin")),
            skip_init_checks=True,
        )
        self.collections_info = {}
        self.health_info = {}
        
    def format_timestamp(self) -> str:
        """Return formatted timestamp for logging"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def print_separator(self, title: str = "", char: str = "-", length: int = 60):
        """Print a formatted separator line"""
        if title:
            print(f"\n{char * 5} {title} {char * (length - len(title) - 12)}")
        else:
            print(char * length)
    
    def check_health(self) -> Dict[str, Any]:
        """Check Weaviate instance health and return status info"""
        print(f"[{self.format_timestamp()}] Checking Weaviate health...")
        
        health_info = {
            'ready': False,
            'live': False,
            'version': 'Unknown',
            'modules': [],
            'cluster_info': None,
            'error': None
        }
        
        try:
            # Basic health checks
            health_info['ready'] = self.client.is_ready()
            health_info['live'] = self.client.is_live()
            
            # Get meta information
            try:
                meta = self.client.get_meta()
                health_info['version'] = meta.get('version', 'Unknown')
                if 'modules' in meta:
                    health_info['modules'] = list(meta['modules'].keys())
            except Exception as e:
                health_info['error'] = f"Meta info error: {e}"
            
            # Try to get cluster information
            try:
                if hasattr(self.client.cluster, 'get_nodes_status'):
                    health_info['cluster_info'] = self.client.cluster.get_nodes_status()
                elif hasattr(self.client.cluster, 'get_nodes'):
                    health_info['cluster_info'] = self.client.cluster.get_nodes()
            except Exception:
                health_info['cluster_info'] = "Single node deployment"
                
        except Exception as e:
            health_info['error'] = str(e)
        
        self.health_info = health_info
        return health_info
    
    def get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """Get comprehensive information about a specific collection"""
        collection_info = {
            'name': collection_name,
            'total_objects': 0,
            'query_time': 0,
            'sample_objects': [],
            'properties': {},
            'property_types': {},
            'vectorizer_config': {},
            'index_config': {},
            'error': None
        }
        
        try:
            collection = self.client.collections.get(collection_name)
            
            # Get total count with timing
            start_time = time.time()
            response = collection.aggregate.over_all(total_count=True)
            collection_info['query_time'] = time.time() - start_time
            collection_info['total_objects'] = response.total_count
            
            # Get sample objects if collection has data
            if collection_info['total_objects'] > 0:
                try:
                    sample_response = collection.query.fetch_objects(limit=2, include_vector=True)
                    sample_objects = sample_response.objects
                    
                    for obj in sample_objects:
                        sample_data = {
                            'uuid': str(obj.uuid),
                            'properties': {},
                            'vectors': {}
                        }
                        
                        # Process properties
                        if obj.properties:
                            for key, value in obj.properties.items():
                                if isinstance(value, str) and len(value) > 150:
                                    sample_data['properties'][key] = value[:150] + "..."
                                elif isinstance(value, (list, dict)):
                                    sample_data['properties'][key] = f"{type(value).__name__}[{len(value)}]"
                                else:
                                    sample_data['properties'][key] = value
                        
                        # Process vectors
                        if hasattr(obj, 'vector') and obj.vector:
                            if isinstance(obj.vector, dict):
                                for vector_name, vector_data in obj.vector.items():
                                    if vector_data:
                                        sample_data['vectors'][vector_name] = len(vector_data)
                            elif isinstance(obj.vector, list):
                                sample_data['vectors']['default'] = len(obj.vector)
                        
                        collection_info['sample_objects'].append(sample_data)
                        
                except Exception as e:
                    collection_info['error'] = f"Sample data error: {e}"
            
            # Get collection configuration
            try:
                config = self.client.collections.get(collection_name).config.get()
                
                # Process properties
                if config.properties:
                    for prop in config.properties:
                        prop_name = prop.name
                        prop_type = prop.data_type[0] if prop.data_type else "Unknown"
                        
                        collection_info['properties'][prop_name] = {
                            'data_type': prop_type,
                            'indexed': getattr(prop, 'index_searchable', None),
                            'filterable': getattr(prop, 'index_filterable', None)
                        }
                        
                        # Group by type
                        if prop_type not in collection_info['property_types']:
                            collection_info['property_types'][prop_type] = []
                        collection_info['property_types'][prop_type].append(prop_name)
                
                # Get vectorizer config
                if hasattr(config, 'vectorizer_config') and config.vectorizer_config:
                    vectorizer = config.vectorizer_config
                    collection_info['vectorizer_config'] = {
                        'type': type(vectorizer).__name__,
                        'details': {}
                    }
                    
                    # Extract common attributes
                    for attr in ['model', 'base_url', 'vectorize_collection_name']:
                        if hasattr(vectorizer, attr):
                            value = getattr(vectorizer, attr, None)
                            if value:
                                collection_info['vectorizer_config']['details'][attr] = value
                
                # Get index config
                if hasattr(config, 'vector_index_config') and config.vector_index_config:
                    index_config = config.vector_index_config
                    collection_info['index_config'] = {
                        'type': type(index_config).__name__,
                        'details': {}
                    }
                    
                    # Extract common attributes
                    for attr in ['distance_metric', 'ef', 'max_connections', 'ef_construction', 'm']:
                        if hasattr(index_config, attr):
                            value = getattr(index_config, attr, None)
                            if value is not None:
                                collection_info['index_config']['details'][attr] = value
                        
            except Exception as e:
                collection_info['error'] = f"Config error: {e}"
                
        except Exception as e:
            collection_info['error'] = f"Collection access error: {e}"
        
        return collection_info
    
    def inspect_all_collections(self) -> Dict[str, Any]:
        """Inspect all collections and return comprehensive information"""
        print(f"[{self.format_timestamp()}] Starting comprehensive collection inspection...")
        
        inspection_result = {
            'timestamp': self.format_timestamp(),
            'collections': {},
            'summary': {
                'total_collections': 0,
                'total_objects': 0,
                'collection_names': []
            },
            'error': None
        }
        
        try:
            # Get all collections
            all_collections = self.client.collections.list_all()
            
            if not all_collections:
                inspection_result['error'] = "No collections found"
                return inspection_result
            
            inspection_result['summary']['total_collections'] = len(all_collections)
            inspection_result['summary']['collection_names'] = list(all_collections.keys())
            
            # Inspect each collection
            for collection_name in all_collections.keys():
                print(f"  Inspecting collection: {collection_name}")
                collection_info = self.get_collection_info(collection_name)
                inspection_result['collections'][collection_name] = collection_info
                inspection_result['summary']['total_objects'] += collection_info['total_objects']
            
            self.collections_info = inspection_result
            return inspection_result
            
        except Exception as e:
            inspection_result['error'] = str(e)
            return inspection_result
    
    def print_health_report(self):
        """Print formatted health report"""
        if not self.health_info:
            self.check_health()
            
        self.print_separator("WEAVIATE HEALTH REPORT", "=")
        
        health = self.health_info
        print(f"✅ Ready: {health['ready']}")
        print(f"✅ Live: {health['live']}")
        print(f"✅ Version: {health['version']}")
        
        if health['cluster_info']:
            if isinstance(health['cluster_info'], str):
                print(f"✅ Cluster: {health['cluster_info']}")
            else:
                print(f"✅ Cluster Nodes: {len(health['cluster_info'])}")
        
        if health['modules']:
            # Categorize modules
            text_modules = [m for m in health['modules'] if 'text2vec' in m]
            multi_modules = [m for m in health['modules'] if 'multi2vec' in m or 'multi2multivec' in m]
            generative_modules = [m for m in health['modules'] if 'generative' in m]
            other_modules = [m for m in health['modules'] if m not in text_modules + multi_modules + generative_modules]
            
            print(f"✅ Modules ({len(health['modules'])} total):")
            if text_modules:
                print(f"   📝 Text Vectorizers: {len(text_modules)}")
            if multi_modules:
                print(f"   🔄 Multi Vectorizers: {len(multi_modules)}")
            if generative_modules:
                print(f"   🤖 Generative: {len(generative_modules)}")
            if other_modules:
                print(f"   🔧 Other: {len(other_modules)}")
        
        if health['error']:
            print(f"⚠️ Errors: {health['error']}")
    
    def print_collection_report(self, collection_name: str = None):
        """Print detailed collection report"""
        if not self.collections_info:
            print("⚠️ No collection data available. Run inspect_all_collections() first.")
            return
        
        collections_to_show = [collection_name] if collection_name else list(self.collections_info['collections'].keys())
        
        for name in collections_to_show:
            if name not in self.collections_info['collections']:
                print(f"❌ Collection '{name}' not found")
                continue
                
            collection = self.collections_info['collections'][name]
            
            self.print_separator(f"COLLECTION: {name}", "*")
            
            print(f"📊 Basic Information:")
            print(f"   Total Objects: {collection['total_objects']:,}")
            print(f"   Query Time: {collection['query_time']:.3f}s")
            print(f"   Properties ({len(collection['properties'])}):")

            # Properties information
            if collection['properties']:
                for prop_name, prop_info in collection['properties'].items():
                    print(f"   • {prop_name}: {prop_info['data_type']}")
                
                # Property type distribution
                if collection['property_types']:
                    print(f"\n📈 Type Distribution:")
                    for prop_type, props in collection['property_types'].items():
                        print(f"   {prop_type}: {len(props)} properties")
            
            # Sample data
            if collection['sample_objects']:
                for i, sample in enumerate(collection['sample_objects']):
                    print(f"\n   Sample {i} (UUID: {sample['uuid'][:8]}...):")
                    if sample['properties']:
                        print(f"     Properties:")
                        for key, value in sample['properties'].items():
                            if key=="content":
                                print(f"       chunk size: {len(value)}")
                    if sample['vectors']:
                        print(f"     Vectors:")
                        for vector_name, dimensions in sample['vectors'].items():
                            print(f"       {vector_name}: {dimensions} dimensions")

            if collection['error']:
                print(f"\n⚠️ Errors: {collection['error']}")
    
    def print_summary(self):
        """Print overall summary"""
        if not self.collections_info:
            print("⚠️ No collection data available.")
            return
            
        self.print_separator("INSPECTION SUMMARY", "=")
        
        summary = self.collections_info['summary']
        print(f"🗂️ Total Collections: {summary['total_collections']}")
        print(f"📄 Total Objects: {summary['total_objects']:,}")
        print(f"⏰ Inspection Time: {self.collections_info['timestamp']}")
        
        if summary['collection_names']:
            print(f"\n📋 Collections Found:")
            for i, name in enumerate(summary['collection_names'], 1):
                collection = self.collections_info['collections'][name]
                print(f"   {i}. {name} ({collection['total_objects']:,} objects)")
        
    
    def get_collection_data(self, collection_name: str) -> Dict[str, Any]:
        """Get raw collection data for programmatic use"""
        if not self.collections_info or collection_name not in self.collections_info['collections']:
            return {}
        return self.collections_info['collections'][collection_name]
    
    def get_all_collection_names(self) -> List[str]:
        """Get list of all collection names"""
        if not self.collections_info:
            return []
        return self.collections_info['summary']['collection_names']
    
    def close(self):
        """Close the Weaviate client connection"""
        if self.client:
            try:
                self.client.close()
                print(f"✅ [{self.format_timestamp()}] Weaviate client connection closed.")
            except Exception as e:
                print(f"⚠️ Warning during client cleanup: {e}")

# Usage Example
def weaviate_monitor():
    """Main function demonstrating usage"""
    print("🔍 Weaviate Collection Inspector")
    print("=" * 50)
    host_type=os.getenv("HOST_TYPE")
    print("HOST_TYPE: ", host_type)
    try:
        host_type = host_type.lower()
        if host_type == "dev":
            host = os.getenv("DEV_HOST")
        elif host_type == "prod":
            host = os.getenv("PROD_HOST")
        elif host_type == "local":
            host = "localhost"
        else:
            raise ValueError(f"Invalid host type: {host_type}")
        

        # Create inspector
        inspector = WeaviateInspector(host)
        
        # Run health check
        inspector.check_health()
        inspector.print_health_report()
        
        # Inspect all collections
        inspector.inspect_all_collections()
        
        # Print reports
        inspector.print_collection_report()
        inspector.print_summary()
        
        # Example of programmatic usage
        all_collections = inspector.get_all_collection_names()
        print(f"\nProgrammatic access - Collections: {all_collections}")
        
        for collection_name in all_collections:
            data = inspector.get_collection_data(collection_name)
            print(f"{collection_name}: {data['total_objects']} objects")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if 'inspector' in locals():
            inspector.close()
