from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from src.config.postgres_config import PG_DB_URL, Base
from src.utils.relationalDB.postgres_utils import DBUtils
from pandas import DataFrame
from typing import Union
from sqlalchemy import select, text
class PostgresController:
    def __init__(self):
        
        self.engine = create_engine(PG_DB_URL, echo=True)
        self.SessionLocal = sessionmaker(bind=self.engine)
        self.utils = DBUtils(engine=self.engine,session_local=self.SessionLocal)

    def create_tables(self):
        """Create all tables in the database."""
        Base.metadata.create_all(bind=self.engine)
        print("✅ Tables created successfully!")

    def get_session(self):
        """Get a new session."""
        return self.SessionLocal()
    
    def get_tables_info(self):
        """Get all table names and details from the database."""
        return self.utils.get_tables_info()
    
    def insert_organization_person(self, data: Union[dict, object],level:str="1",source:str="",origin:str="s3_bucket") -> int:
        """Insert organization and its representative person into the database."""
        return self.utils.insert_organization_with_person(data,source=source,origin=origin,level=level)
    
    def get_table_view(self,model_schema, limit: int = 5) :
        """Fetch organizations using proper session management."""
        with self.SessionLocal() as session:
            try:
                stmt = select(model_schema).limit(limit)
                result = session.execute(stmt).all()
                return result
            except Exception as e:
                print(f"Error fetching {model_schema}: {e}")
                raise
    def insert_data(self, data: Union[dict, object], model_schema):
        """Insert data into the specified table."""
        return self.utils.insert_data(data, model_schema)
    
    def insert_df(self,df:DataFrame,table_name:str,index:bool=False):
        try:
            df.to_sql(table_name, self.engine, if_exists="append", index=index)
        except Exception as e:
            print(f"❌ Error in df insertion : {e}")
    def delete_all_collections(self, confirm: bool = False):
            """
            Delete (truncate) all tables in the current PostgreSQL database.
            - confirm=True to proceed, False to cancel.
            """
            if not confirm:
                print("⚠️  Deletion not confirmed. Pass confirm=True to execute.")
                return

            with self.engine.connect() as connection:
                transaction = connection.begin()
                try:
                    inspector = inspect(self.engine)
                    tables = inspector.get_table_names()

                    if not tables:
                        print("ℹ️ No tables found in the database.")
                        return

                    # Disable FK constraints for truncation
                    connection.execute(text("SET session_replication_role = 'replica';"))

                    for table in tables:
                        connection.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"))
                        print(f"🗑️  Truncated table: {table}")

                    connection.execute(text("SET session_replication_role = 'origin';"))
                    transaction.commit()
                    print("✅ All tables truncated successfully!")

                except Exception as e:
                    transaction.rollback()
                    print(f"❌ Error truncating tables: {e}")