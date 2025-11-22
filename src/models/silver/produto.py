from sqlalchemy import Integer
from sqlmodel import Column, Field, SQLModel


class Produto(SQLModel, table=True):

    __tablename__ = "produto"

    produto_id: int = Field(sa_column=Column(Integer(), primary_key=True))
    produto: str = Field(default=None, nullable=True)
    regiao: str = Field(default=None, nullable=True)
    safra: str = Field(default=None, nullable=True)
