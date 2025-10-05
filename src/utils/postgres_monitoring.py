import psycopg2
from psycopg2.extras import RealDictCursor
import time
from datetime import datetime
from typing import Dict, List, Any, Optional
from dotenv import load_dotenv
import os
from urllib.parse import urlparse

class PostgreSQLMonitor:
    """A comprehensive PostgreSQL monitor for public schema (excluding alembic_version)"""
    
    def __init__(self, host: str = None):
        """Initialize with database URL or use environment variables"""
        load_dotenv()
        
        self.db_url = host or os.getenv('DEV_HOST')
        self.connection = None
        self.database_info = {}
        self.health_info = {}
        
        # Parse and set connection parameters
        self.connection_params = self._parse_database_url()
        
    def _parse_database_url(self) -> Dict[str, Any]:
        """Parse database URL into connection parameters"""
        try:
            # If db_url is a full PostgreSQL URL (postgresql://...)
            if self.db_url and self.db_url.startswith(('postgresql://', 'postgres://')):
                parsed = urlparse(self.db_url)
                return {
                    'host': parsed.hostname,
                    'port': parsed.port or 5432,
                    'database': parsed.path.lstrip('/'),
                    'user': parsed.username,
                    'password': parsed.password
                }
            # Otherwise use environment variables or db_url as host
            else:
                return {
                    'host': self.db_url or os.getenv('DEV_HOST', 'localhost'),
                    'port': int(os.getenv('POSTGRES_PORT', 5432)),
                    'database': os.getenv('POSTGRES_DB'),
                    'user': os.getenv('POSTGRES_USER'),
                    'password': os.getenv('POSTGRES_PASSWORD')
                }
            
        except Exception as e:
            raise ValueError(f"Invalid database URL format: {e}")
    
    def format_timestamp(self) -> str:
        """Return formatted timestamp for logging"""
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    def print_separator(self, title: str = "", char: str = "-", length: int = 60):
        """Print a formatted separator line"""
        if title:
            print(f"\n{char * 5} {title} {char * (length - len(title) - 12)}")
        else:
            print(char * length)
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL"""
        try:
            print(f"[{self.format_timestamp()}] Connecting to PostgreSQL...")
            print(f"   Host: {self.connection_params['host']}")
            print(f"   Database: {self.connection_params['database']}")
            print(f"   User: {self.connection_params['user']}")
            
            self.connection = psycopg2.connect(**self.connection_params)
            # Set autocommit mode immediately after connection
            self.connection.autocommit = True
            print(f"✅ [{self.format_timestamp()}] Successfully connected to PostgreSQL")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    def check_health(self) -> Dict[str, Any]:
        """Check PostgreSQL health and return status info"""
        print(f"[{self.format_timestamp()}] Checking PostgreSQL health...")
        
        health_info = {
            'connected': False,
            'version': 'Unknown',
            'uptime': 'Unknown',
            'active_connections': 0,
            'max_connections': 0,
            'database_size': 0,
            'cache_hit_ratio': 0.0,
            'index_hit_ratio': 0.0,
            'replica_status': None,
            'error': None
        }
        
        if not self.connection:
            if not self.connect():
                health_info['error'] = "Could not establish connection"
                return health_info
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                health_info['connected'] = True
                
                # Get version
                cursor.execute("SELECT version();")
                version_info = cursor.fetchone()['version']
                health_info['version'] = version_info.split(' ')[1] if version_info else 'Unknown'
                
                # Get uptime
                cursor.execute("SELECT now() - pg_postmaster_start_time() as uptime;")
                uptime = cursor.fetchone()['uptime']
                health_info['uptime'] = str(uptime).split('.')[0] if uptime else 'Unknown'
                
                # Get connection info
                cursor.execute("SELECT setting::int as max_conn FROM pg_settings WHERE name = 'max_connections';")
                max_conn = cursor.fetchone()
                health_info['max_connections'] = max_conn['max_conn'] if max_conn else 0
                
                cursor.execute("SELECT count(*) as active_conn FROM pg_stat_activity WHERE state = 'active';")
                active_conn = cursor.fetchone()
                health_info['active_connections'] = active_conn['active_conn'] if active_conn else 0
                
                # Get database size
                cursor.execute("""
                    SELECT pg_size_pretty(pg_database_size(current_database())) as db_size,
                           pg_database_size(current_database()) as db_size_bytes;
                """)
                size_info = cursor.fetchone()
                health_info['database_size'] = size_info['db_size'] if size_info else '0 bytes'
                health_info['database_size_bytes'] = size_info['db_size_bytes'] if size_info else 0
                
                # Get cache hit ratios
                cursor.execute("""
                    SELECT 
                        COALESCE(round(
                            (sum(heap_blks_hit) / NULLIF(sum(heap_blks_hit + heap_blks_read), 0)) * 100, 2
                        ), 0) as cache_hit_ratio
                    FROM pg_statio_user_tables;
                """)
                cache_ratio = cursor.fetchone()
                health_info['cache_hit_ratio'] = float(cache_ratio['cache_hit_ratio'] or 0)
                
                # Get index hit ratio
                cursor.execute("""
                    SELECT 
                        COALESCE(round(
                            (sum(idx_blks_hit) / NULLIF(sum(idx_blks_hit + idx_blks_read), 0)) * 100, 2
                        ), 0) as index_hit_ratio
                    FROM pg_statio_user_indexes;
                """)
                index_ratio = cursor.fetchone()
                health_info['index_hit_ratio'] = float(index_ratio['index_hit_ratio'] or 0)
                
                # Check if it's a replica
                try:
                    cursor.execute("SELECT pg_is_in_recovery();")
                    is_replica = cursor.fetchone()['pg_is_in_recovery']
                    health_info['replica_status'] = 'Replica' if is_replica else 'Primary'
                except Exception:
                    health_info['replica_status'] = 'Unknown'
                
        except Exception as e:
            health_info['error'] = str(e)
        
        self.health_info = health_info
        return health_info
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get comprehensive information about a specific table in public schema"""
        table_info = {
            'schema': 'public',
            'table': table_name,
            'row_count': 0,
            'table_size': '0 bytes',
            'table_size_bytes': 0,
            'index_size': '0 bytes',
            'index_size_bytes': 0,
            'total_size': '0 bytes',
            'total_size_bytes': 0,
            'query_time': 0,
            'columns': {},
            'indexes': [],
            'constraints': [],
            'sample_data': [],
            'statistics': {},
            'error': None
        }
        
        if not self.connection:
            table_info['error'] = "No database connection"
            return table_info
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Connection already has autocommit = True set in connect()
                
                full_table_name = f'"public"."{table_name}"'
                
                # Get row count with timing
                start_time = time.time()
                cursor.execute(f"SELECT count(*) as row_count FROM {full_table_name};")
                count_result = cursor.fetchone()
                table_info['query_time'] = time.time() - start_time
                table_info['row_count'] = count_result['row_count'] if count_result else 0
                
                # Get table sizes with error handling
                try:
                    cursor.execute("""
                        SELECT 
                            COALESCE(pg_size_pretty(pg_total_relation_size(%s)), '0 bytes') as total_size,
                            COALESCE(pg_total_relation_size(%s), 0) as total_size_bytes,
                            COALESCE(pg_size_pretty(pg_relation_size(%s)), '0 bytes') as table_size,
                            COALESCE(pg_relation_size(%s), 0) as table_size_bytes,
                            COALESCE(pg_size_pretty(pg_indexes_size(%s)), '0 bytes') as index_size,
                            COALESCE(pg_indexes_size(%s), 0) as index_size_bytes
                    """, [full_table_name] * 6)
                    
                    size_info = cursor.fetchone()
                    if size_info:
                        table_info.update({
                            'total_size': size_info['total_size'],
                            'total_size_bytes': size_info['total_size_bytes'],
                            'table_size': size_info['table_size'],
                            'table_size_bytes': size_info['table_size_bytes'],
                            'index_size': size_info['index_size'],
                            'index_size_bytes': size_info['index_size_bytes']
                        })
                except Exception as size_error:
                    print(f"   ⚠️  Size calculation failed for {table_name}: {size_error}")
                
                # Get column information
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = %s
                    ORDER BY ordinal_position;
                """, (table_name,))
                
                columns = cursor.fetchall()
                for col in columns:
                    col_info = {
                        'type': col['data_type'],
                        'nullable': col['is_nullable'] == 'YES',
                        'default': col['column_default']
                    }
                    
                    if col['character_maximum_length']:
                        col_info['max_length'] = col['character_maximum_length']
                    if col['numeric_precision']:
                        col_info['precision'] = col['numeric_precision']
                        col_info['scale'] = col['numeric_scale']
                    
                    table_info['columns'][col['column_name']] = col_info
                
                # Get index information
                cursor.execute("""
                    SELECT 
                        indexname,
                        indexdef,
                        CASE WHEN indisunique THEN 'UNIQUE' ELSE 'INDEX' END as index_type
                    FROM pg_indexes pi
                    JOIN pg_class pc ON pc.relname = pi.indexname
                    JOIN pg_index pgi ON pgi.indexrelid = pc.oid
                    WHERE schemaname = 'public' AND tablename = %s;
                """, (table_name,))
                
                indexes = cursor.fetchall()
                for idx in indexes:
                    table_info['indexes'].append({
                        'name': idx['indexname'],
                        'type': idx['index_type'],
                        'definition': idx['indexdef']
                    })
                
                # Get constraints with fixed query
                cursor.execute("""
                    SELECT 
                        tc.constraint_name,
                        tc.constraint_type,
                        kcu.column_name
                    FROM information_schema.table_constraints tc
                    LEFT JOIN information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                        AND tc.table_name = kcu.table_name
                    WHERE tc.table_schema = 'public' AND tc.table_name = %s;
                """, (table_name,))
                
                constraints = cursor.fetchall()
                constraint_dict = {}
                for constraint in constraints:
                    name = constraint['constraint_name']
                    if name not in constraint_dict:
                        constraint_dict[name] = {
                            'type': constraint['constraint_type'],
                            'columns': []
                        }
                    if constraint['column_name']:
                        constraint_dict[name]['columns'].append(constraint['column_name'])
                
                table_info['constraints'] = list(constraint_dict.values())
                
                # Get sample data if table has rows
                if table_info['row_count'] > 0:
                    try:
                        cursor.execute(f"SELECT * FROM {full_table_name} LIMIT 3;")
                        samples = cursor.fetchall()
                        
                        for sample in samples:
                            sample_data = {}
                            for key, value in sample.items():
                                if isinstance(value, str) and len(value) > 150:
                                    sample_data[key] = value[:150] + "..."
                                elif isinstance(value, (list, dict)):
                                    sample_data[key] = f"{type(value).__name__}[{len(value)}]"
                                else:
                                    sample_data[key] = value
                            table_info['sample_data'].append(sample_data)
                            
                    except Exception as e:
                        print(f"   ⚠️  Sample data error for {table_name}: {e}")
                
                # Get table statistics
                cursor.execute("""
                    SELECT 
                        seq_scan,
                        seq_tup_read,
                        idx_scan,
                        idx_tup_fetch,
                        n_tup_ins,
                        n_tup_upd,
                        n_tup_del,
                        n_live_tup,
                        n_dead_tup,
                        last_vacuum,
                        last_autovacuum,
                        last_analyze,
                        last_autoanalyze
                    FROM pg_stat_user_tables 
                    WHERE schemaname = 'public' AND relname = %s;
                """, (table_name,))
                
                stats = cursor.fetchone()
                if stats:
                    table_info['statistics'] = dict(stats)
                
        except Exception as e:
            # No need to rollback since we're in autocommit mode
            table_info['error'] = str(e)
        
        return table_info
    
    def inspect_public_tables(self) -> Dict[str, Any]:
        """Inspect all tables in public schema except alembic_version"""
        print(f"[{self.format_timestamp()}] Starting public schema inspection (excluding alembic_version)...")
        
        inspection_result = {
            'timestamp': self.format_timestamp(),
            'schema': 'public',
            'tables': {},
            'summary': {
                'total_tables': 0,
                'total_rows': 0,
                'total_size_bytes': 0,
                'table_names': []
            },
            'error': None
        }
        
        if not self.connection:
            if not self.connect():
                inspection_result['error'] = "Could not establish connection"
                return inspection_result
        
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get all tables in public schema except alembic_version
                cursor.execute("""
                    SELECT table_name
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_type = 'BASE TABLE'
                    AND table_name != 'alembic_version'
                    ORDER BY table_name;
                """)
                
                tables = cursor.fetchall()
                
                if not tables:
                    inspection_result['error'] = "No tables found in public schema (excluding alembic_version)"
                    return inspection_result
                
                table_names = [table['table_name'] for table in tables]
                inspection_result['summary']['total_tables'] = len(table_names)
                inspection_result['summary']['table_names'] = table_names
                
                # Inspect each table
                for table_name in table_names:
                    print(f"  Inspecting table: {table_name}")
                    table_info = self.get_table_info(table_name)
                    inspection_result['tables'][table_name] = table_info
                    inspection_result['summary']['total_rows'] += table_info.get('row_count', 0)
                    inspection_result['summary']['total_size_bytes'] += table_info.get('total_size_bytes', 0)
                
                self.database_info = inspection_result
                return inspection_result
                
        except Exception as e:
            inspection_result['error'] = str(e)
            return inspection_result
    
    def print_health_report(self):
        """Print formatted health report"""
        if not self.health_info:
            self.check_health()
        
        self.print_separator("POSTGRESQL HEALTH REPORT", "=")
        
        health = self.health_info
        print(f"✅ Connected: {health['connected']}")
        print(f"✅ Version: {health['version']}")
        print(f"✅ Uptime: {health['uptime']}")
        print(f"✅ Role: {health['replica_status']}")
        print(f"📊 Connections: {health['active_connections']}/{health['max_connections']}")
        print(f"💾 Database Size: {health['database_size']}")
        print(f"🎯 Cache Hit Ratio: {health['cache_hit_ratio']:.1f}%")
        print(f"📇 Index Hit Ratio: {health['index_hit_ratio']:.1f}%")
        
    
        
        if health['error']:
            print(f"❌ Errors: {health['error']}")
    
    def print_table_reports(self):
        """Print detailed reports for all tables"""
        if not self.database_info:
            print("⚠️ No database data available. Run inspect_public_tables() first.")
            return
        
        for table_name, table_info in self.database_info['tables'].items():
            self.print_separator(f"TABLE: public.{table_name}", "*")
            
            print(f"📊 Basic Information:")
            print(f"   Total Rows: {table_info.get('row_count', 0):,}")
            print(f"   Total Columns: {len(table_info.get('columns', {}))}")
            print(f"   Table Size: {table_info.get('table_size', '0 bytes')}")
            print(f"   Index Size: {table_info.get('index_size', '0 bytes')}")
            print(f"   Total Size: {table_info.get('total_size', '0 bytes')}")
            print(f"   Query Time: {table_info.get('query_time', 0):.3f}s")

            
            if table_info.get('error'):
                print(f"\n⚠️ Errors: {table_info['error']}")
    
    def print_summary(self):
        """Print overall summary"""
        if not self.database_info:
            print("⚠️ No database data available.")
            return
        
        self.print_separator("INSPECTION SUMMARY", "=")
        
        summary = self.database_info['summary']
        total_size_mb = summary['total_size_bytes'] / (1024 * 1024) if summary['total_size_bytes'] > 0 else 0
        
        print(f"📄 Total Tables: {summary['total_tables']}")
        print(f"📊 Total Rows: {summary['total_rows']:,}")
        print(f"💾 Total Size: {total_size_mb:.2f} MB")
        print(f"⏰ Inspection Time: {self.database_info['timestamp']}")
        
        if summary['table_names']:
            print(f"\n📋 Tables Found:")
            for table_name in summary['table_names']:
                table_info = self.database_info['tables'][table_name]
                row_count = table_info.get('row_count', 0)
                table_size = table_info.get('table_size', '0 bytes')
                print(f"   • {table_name}: {row_count:,} rows ({table_size})")

    
    def close(self):
        """Close the PostgreSQL connection"""
        if self.connection:
            try:
                self.connection.close()
                print(f"✅ [{self.format_timestamp()}] PostgreSQL connection closed.")
            except Exception as e:
                print(f"⚠️ Warning during connection cleanup: {e}")


def monitor_public_tables():
    """Main monitoring function for public tables (excluding alembic_version)"""
    
    monitor = None
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
        # Create monitor
        monitor = PostgreSQLMonitor(host=host)
        
        # Run health check
        health = monitor.check_health()
        if not health.get('connected'):
            print(f"❌ Health check failed: {health.get('error', 'Unknown error')}")
            return
        
        monitor.print_health_report()
        
        # Inspect public tables
        result = monitor.inspect_public_tables()
        
        if result.get('error'):
            print(f"❌ Inspection failed: {result['error']}")
            return
        
        # Print reports
        monitor.print_table_reports()
        monitor.print_summary()
        
        
        print("\n🎉 Monitoring completed successfully!")
        
    except Exception as e:
        print(f"❌ Monitoring failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if monitor:
            monitor.close()
