# models.py
from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.orm import relationship
from src.config.postgres_config import Base

class Organization(Base):
    __tablename__ = "organizations"

    id = Column(Integer, primary_key=True, autoincrement=True)
    organization_name = Column(String)
    company_overview = Column(Text)
    business_activities = Column(Text)
    history = Column(Text)
    group_companies = Column(Text)
    major_business_partners = Column(Text)
    sales_trends = Column(Text)
    president_message = Column(Text)
    interview_articles = Column(Text)
    past_transactions = Column(Text)

    # One-to-one or one-to-many depending on your requirement
    person = relationship("Person", back_populates="organization", uselist=False)
