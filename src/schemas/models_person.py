# models.py
from sqlalchemy import Column, Integer, String, Text, ForeignKey
from sqlalchemy.orm import relationship
from src.config.postgres_config import Base

class Person(Base):
    __tablename__ = "persons"

    id = Column(Integer, primary_key=True, autoincrement=True)
    person_name = Column(String)
    title = Column(String)
    career_history = Column(Text)
    current_activities = Column(Text)
    publications = Column(Text)
    source = Column(Text)
    origin = Column(Text)
    level = Column(Text)
    organization_id = Column(Integer, ForeignKey("organizations.id", ondelete="SET NULL"))

    organization = relationship("Organization", back_populates="person")
