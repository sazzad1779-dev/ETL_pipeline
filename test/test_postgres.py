
from src.schemas.models_organization import Organization
from src.controller.postgres_controller import PostgresController

db = PostgresController()
db.get_session()

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from src.config.postgres_config import PG_DB_URL
from src.schemas.models_organization import Organization
from src.schemas.models_person import Person

# Sample data for organizations
sample_organizations = [
    {
        "organization_name": "TechNova Solutions",
        "company_overview": "Leading technology solutions provider specializing in AI and cloud computing services for enterprise clients.",
        "business_activities": "Software development, AI consulting, cloud migration services, data analytics, cybersecurity solutions.",
        "history": "Founded in 2018 by former Google engineers. Started as a small AI startup and grew to 500+ employees. IPO in 2023.",
        "group_companies": "TechNova Labs (R&D), CloudSecure Inc. (Security), DataFlow Analytics (Big Data subsidiary).",
        "major_business_partners": "Microsoft Azure, Amazon Web Services, Google Cloud Platform, Oracle, Salesforce.",
        "sales_trends": "2022: $50M revenue (40% growth), 2023: $75M revenue (50% growth), 2024: $110M revenue (47% growth).",
        "president_message": "We believe technology should empower human potential. Our mission is to make AI accessible to every business.",
        "interview_articles": "Featured in TechCrunch (2023), Forbes 30 Under 30 CEO profile, MIT Technology Review AI innovation spotlight.",
        "past_transactions": "Series A: $10M (2019), Series B: $25M (2021), IPO: $200M (2023), Acquired DataFlow for $15M (2024)."
    },
    {
        "organization_name": "Green Energy Corp",
        "company_overview": "Renewable energy company focused on solar and wind power solutions for residential and commercial markets.",
        "business_activities": "Solar panel manufacturing, wind turbine installation, energy storage systems, green consulting services.",
        "history": "Established in 2015 during the renewable energy boom. Expanded internationally in 2020. Now operates in 15 countries.",
        "group_companies": "SolarTech Manufacturing, WindPower Installations, EcoStorage Systems, GreenConsult Advisory.",
        "major_business_partners": "Tesla Energy, Siemens, General Electric, First Solar, Vestas Wind Systems.",
        "sales_trends": "2022: $80M revenue (25% growth), 2023: $95M revenue (19% growth), 2024: $120M revenue (26% growth).",
        "president_message": "The future is renewable. We're committed to making clean energy affordable and accessible worldwide.",
        "interview_articles": "Bloomberg Green feature story, Reuters sustainability interview, Wall Street Journal renewable energy profile.",
        "past_transactions": "Government grant: $5M (2016), Private equity: $30M (2019), Green bonds: $50M (2022)."
    }
]

# db.create_tables()
# db.insert_organization_person(sample_organizations[0])

organizations = db.get_table_view(Organization,5)
print(f"Fetched {len(organizations)} organizations:")
for org in organizations:
    print("organization id: ", org[0].id, org[0].organization_name)

