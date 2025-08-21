from agentic_doc.parse import parse
from typing import Optional, Type
from pydantic import BaseModel, Field


class PersonExtractedFields(BaseModel):
    person_name: str = Field(description="The full name of the person")
    organization: str = Field(description="The organization or company the person is affiliated with")
    title: str = Field(description="The title or position held by the person")
    career_history: str = Field(description="A summary of the person's past roles or professional experience")
    current_activities: str = Field(description="The person's current responsibilities, projects, or roles")
    publications: str = Field(description="List or summary of the person's publications or patents, if any")


class OrganizationExtractedFields(BaseModel):
    organization_name: str = Field(description="The full name of the organization")
    company_overview: str = Field(description="A general overview of the company and its purpose.")
    business_activities: str = Field(description="Description of the company's core business operations")
    history: str = Field(description="A summary of the company's history and founding background")
    group_companies: str = Field(description="List or overview of subsidiaries or group companies")
    major_business_partners: str = Field(description="Key business partners and affiliations")
    sales_trends: str = Field(description="Sales trends and financial performance over time")
    president_message: str = Field(description="Message or statement from the company's president or CEO")
    interview_articles: str = Field(description="Related interviews or featured articles about the company")
    past_transactions: str = Field(description="Notable transactions the company has been involved in")
    representative_persons:PersonExtractedFields = Field(
        description="All information about representative individuals of the organization."
    )

