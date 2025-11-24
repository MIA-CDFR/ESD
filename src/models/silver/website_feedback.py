from datetime import datetime

from sqlalchemy import Integer
from sqlmodel import Column, Field, SQLModel


class WebsiteFeedback(SQLModel, table=True):

    __tablename__ = "website_feedback"

    id: int = Field(sa_column=Column(Integer(), primary_key=True))
    object_id: str = Field(default=None, nullable=True)
    email: str = Field(default=None, nullable=True)
    comentarios: str = Field(default=None, nullable=True)
    classificacao: int = Field(default=None, nullable=True)
    datahora: datetime = Field(default=None, nullable=True)
    source: str = Field(default='website feedback', nullable=True)
    extracted: bool = Field(default=None, nullable=True)
