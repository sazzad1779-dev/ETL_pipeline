from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.orm import sessionmaker
from src.config.postgres_config import PG_DB_URL, Base
from src.utils.relationalDB.postgres_utils import DBUtils
from pandas import DataFrame
from typing import Union
import pandas as pd
import hashlib

from src.utils.logger_config import logger 


class PostgresController:
    def __init__(self):
        logger.info("Initializing PostgresController...")
        try:
            self.engine = create_engine(PG_DB_URL, echo=False)
            self.SessionLocal = sessionmaker(bind=self.engine)
            self.utils = DBUtils(engine=self.engine, session_local=self.SessionLocal)
        except Exception as e:
            logger.error(f"Failed to initialize PostgresController: {e}")
            raise

    
    def create_tables(self):
        """Create all tables in the database."""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.success("Tables created successfully.")
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise

    
    def get_session(self):
        """Get a new session."""
        return self.SessionLocal()

    
    def get_tables_info(self):
        """Get metadata details of all tables."""
        return self.utils.get_tables_info()

    
    def insert_organization_person(
        self, data: Union[dict, object], level: str = "1",
        source: str = "", origin: str = "s3_bucket"
    ) -> int:
        """Insert organization + person."""
        logger.info(
            f"Inserting organization/person | level={level}, source={source}, origin={origin}"
        )
        return self.utils.insert_organization_with_person(
            data, source=source, origin=origin, level=level
        )

    
    def get_table_view(self, model_schema, limit: int = 5):
        """Fetch table data with limit."""
        logger.info(f"Fetching first {limit} rows from table: {model_schema.__tablename__}")

        with self.SessionLocal() as session:
            try:
                stmt = select(model_schema).limit(limit)
                result = session.execute(stmt).all()
                return result
            except Exception as e:
                logger.error(f"Error fetching table view ({model_schema}): {e}")
                raise
    
    def insert_df(self, df: DataFrame, table_name: str, index: bool = False):
        """
        Insert a pandas DataFrame into PostgreSQL safely with duplicate prevention.
        
        Duplicate rows are detected via a canonical MD5 hash of the content,
        excluding metadata fields: 'source', 'origin', 'level', and 'index'.
        """
        logger.info(f"Starting DataFrame insertion into '{table_name}' | rows={len(df)}")
        try:
            # -------------------------
            # Step 1: Define columns to ignore for hashing
            # -------------------------
            ignore_cols = {"source", "origin", "level", "index", "_content_hash"}
            hash_cols = sorted([c for c in df.columns if c not in ignore_cols])

            if not hash_cols:
                logger.warning("No columns left to hash after excluding metadata.")
                return

            # -------------------------
            # Step 2: Canonicalize values and compute hash
            # -------------------------
            def canonicalize(val):
                if pd.isna(val):
                    return ""
                if isinstance(val, (int, float)):
                    return str(val).strip()
                if isinstance(val, str):
                    return val.strip()
                return str(val).strip()

            logger.debug("Computing canonical row hashes...")
            df["_content_hash"] = df[hash_cols].apply(
                lambda row: hashlib.md5("|".join(canonicalize(v) for v in row.values).encode("utf-8")).hexdigest(),
                axis=1
            )

            # -------------------------
            # Step 3: Ensure _content_hash column exists
            # -------------------------
            logger.debug("Ensuring '_content_hash' column exists in the table...")
            with self.engine.begin() as conn:
                conn.execute(text(f"""
                    ALTER TABLE {table_name} 
                    ADD COLUMN IF NOT EXISTS _content_hash TEXT UNIQUE;
                """))

            # -------------------------
            # Step 4: Filter duplicates against existing table
            # -------------------------
            logger.debug("Fetching existing hashes from the database...")
            existing_hashes = pd.read_sql(f"SELECT _content_hash FROM {table_name}", self.engine)
            before_filter = len(df)
            df = df.drop_duplicates(subset=["_content_hash"])
            df = df[~df["_content_hash"].isin(existing_hashes["_content_hash"])]
            new_rows = len(df)
            logger.info(f"Filtered {before_filter - new_rows} duplicate rows. New rows to insert: {new_rows}")

            # -------------------------
            # Step 5: Insert new rows
            # -------------------------
            if new_rows > 0:
                # Ensure we do not insert DataFrame index as a column
                df.to_sql(table_name, self.engine, if_exists="append", index=False)
                logger.success(f"Inserted {new_rows} rows into '{table_name}'.")
            else:
                logger.warning(f"No new data to insert for table '{table_name}'.")

        except Exception as e:
            logger.error(f"Error inserting DataFrame into '{table_name}': {e}")
            raise



    def delete_collections(self, tables=None, confirm=False):
        """
        Delete (truncate) one, multiple, or all tables in PostgreSQL.
        
        Args:
            tables (str | list | None):
                - None  → delete ALL tables
                - "table_name" → delete one table
                - ["t1", "t2"] → delete multiple tables
            confirm (bool): Set to True to allow deletion.
        """

        if not confirm:
            logger.warning("Deletion aborted — confirm=True required.")
            return

        try:
            inspector = inspect(self.engine)
            all_tables = inspector.get_table_names()

            # Determine which tables to truncate
            if tables is None:
                target_tables = all_tables
            else:
                # Normalize input
                if isinstance(tables, str):
                    tables = [tables]

                # Validate table names
                invalid = [t for t in tables if t not in all_tables]
                if invalid:
                    logger.error(f"Invalid table(s): {invalid}")
                    return

                target_tables = tables
                logger.info(f"Deleting tables: {target_tables}")

            if not target_tables:
                logger.warning("No tables found to delete.")
                return

            with self.engine.connect() as connection:
                transaction = connection.begin()

                # Disable FK constraints (PostgreSQL trick)
                connection.execute(text("SET session_replication_role = 'replica';"))

                for table in target_tables:
                    connection.execute(text(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"))
                    logger.success(f"Truncated table: {table}")

                # Restore FK constraint behavior
                connection.execute(text("SET session_replication_role = 'origin';"))
                transaction.commit()

                logger.info(" Deletion completed successfully.")

        except Exception as e:
            logger.error(f"Error deleting tables: {e}")
            raise
