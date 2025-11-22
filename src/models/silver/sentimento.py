from datetime import datetime
from sqlalchemy import Integer
from sqlmodel import Column, Field, SQLModel


class Sentimento(SQLModel, table=True):

    __tablename__ = "sentimento"

    sentimento_id: int = Field(sa_column=Column(Integer(), primary_key=True))
    utilizador_id: int = Field(default=None, nullable=True)
    text: str = Field(default=None, nullable=True)
    datahora: datetime = Field(default=None, nullable=True)
    modelo: str = Field(default=None, nullable=True)
    sentimento: str = Field(default=None, nullable=True)
    score: float = Field(default=None, nullable=True)
    created_on: datetime = Field(default=None, nullable=True)
