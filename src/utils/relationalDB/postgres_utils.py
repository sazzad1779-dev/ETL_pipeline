from src.schemas.models_person import Person
from src.schemas.models_organization import Organization
from typing import Union,Optional,List
from sqlalchemy import inspect

class DBUtils:
    def __init__(self,engine,  session_local):
        self.SessionLocal = session_local
        self.engine = engine

    def add_organization(session, name, country=None):
        org = Organization(name=name, country=country)
        session.add(org)
        session.commit()
        session.refresh(org)
        return org

    
    def add_person(session, name, title=None, organization_id=None):
        person = Person(name=name, title=title, organization_id=organization_id)
        session.add(person)
        session.commit()
        session.refresh(person)
        return person
    def _to_dict(self, data: Union[dict, object]) -> dict:
        """Convert Pydantic or object with dict/model_dump to plain dict."""
        if isinstance(data, dict):
            return data
        if hasattr(data, "model_dump"):
            return data.model_dump()
        if hasattr(data, "dict"):
            return data.dict()
        raise TypeError("Data must be a dict or have dict()/model_dump() method")
    
    def insert_organization_with_person(self, data: Union[dict, object],level:str="1",source:str="",origin:str="s3_bucket") -> int:
        data = self._to_dict(data)
        person_data = data.pop("representative_persons", None)

        with self.SessionLocal() as session:
            # Create organization first
            org = Organization(**data)
            if isinstance(org, dict):
                org["origin"] = origin
                org["source"] = source
                org["level"] = level
            else:  # SQLAlchemy ORM object
                org.origin = origin
                org.source = source
                org.level = level
            session.add(org)
            session.flush()  # This assigns the ID to org
            if person_data:
                person_data = self._to_dict(person_data)
                person_data.pop("organization", None)
                # Set the foreign key to link person to organization
                person_data['organization_id'] = org.id
                person_data['origin'] = origin
                person_data["source"] = source
                person_data["level"] = level
                person = Person(**person_data)
                session.add(person)
            session.commit()
            return org.id
        
    def view_organization(self, org_id: Optional[int] = None, limit: Optional[int] = None) -> List[dict]:
        """Fetch one or all organizations (with their persons)."""
        with self.SessionLocal() as session:
            query = session.query(Organization)
            if org_id:
                query = query.filter(Organization.id == org_id)
            if limit:
                query = query.limit(limit)

            organizations = query.all()
            print("Organizations:", organizations)

            result = []
            for org in organizations:
                result.append({
                    "id": org.id,
                    "name": org.name,
                    "address": org.address,
                    "representative_persons": [
                        {
                            "id": p.id,
                            "name": p.name,
                            "title": p.title
                        }
                        for p in org.persons
                    ]
                })
            return result

        
    def get_tables_info(self):
        """Get all table names and details from the database."""
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        result = {}

        for table in tables:
            result[table] = {
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col["nullable"],
                        "default": col.get("default")
                    }
                    for col in inspector.get_columns(table)
                ],
                "primary_key": inspector.get_pk_constraint(table),
                "foreign_keys": inspector.get_foreign_keys(table),
                "indexes": inspector.get_indexes(table)
            }

        return result
