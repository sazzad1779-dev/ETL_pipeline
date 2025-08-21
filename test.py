
from src.schemas.models_organization import Organization
from src.controller.postgres_controller import PostgresController

db = PostgresController()
db.get_session()

organizations = db.get_organizations(5)
print(f"Fetched {len(organizations)} organizations:")
for org in organizations:
    print("organization id: ", org[0].id, org[0].organization_name)