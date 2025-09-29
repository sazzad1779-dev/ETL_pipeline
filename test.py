import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

def simple_test():
    """Simple test to check if connection works"""
    load_dotenv()
    
    # Your database URL
    db_url = os.getenv('PG_DB_URL', 'localhost')
    
    # Parse connection parameters
    from urllib.parse import urlparse
    
    if '+psycopg2://' in db_url:
        clean_url = db_url.replace('postgresql+psycopg2://', 'postgresql://')
    else:
        clean_url = db_url
        
    parsed = urlparse(clean_url)
    
    conn_params = {
        'host': parsed.hostname,
        'port': parsed.port or 5432,
        'database': parsed.path.lstrip('/'),
        'user': parsed.username,
        'password': parsed.password
    }
    
    print("Connection parameters:")
    print(f"  Host: {conn_params['host']}")
    print(f"  Port: {conn_params['port']}")
    print(f"  Database: {conn_params['database']}")
    print(f"  User: {conn_params['user']}")
    
    try:
        # Test connection
        conn = psycopg2.connect(**conn_params)
        print("✅ Connection successful!")
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Check what schemas exist
            cur.execute("""
                SELECT schema_name 
                FROM information_schema.schemata 
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
            """)
            schemas = cur.fetchall()
            print(f"\nAvailable schemas: {[s['schema_name'] for s in schemas]}")
            
            # Check tables in each schema
            for schema in schemas:
                schema_name = schema['schema_name']
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                """, (schema_name,))
                tables = cur.fetchall()
                print(f"  {schema_name}: {len(tables)} tables")
                if tables:
                    for table in tables[:3]:  # Show first 3 tables
                        print(f"    - {table['table_name']}")
                    if len(tables) > 3:
                        print(f"    ... and {len(tables) - 3} more")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    simple_test()