from datetime import date
from sqlalchemy import Integer
from sqlmodel import Column, Field, SQLModel


class Utilizador(SQLModel, table=True):

    __tablename__ = "utilizador"

    utilizador_id: int = Field(sa_column=Column(Integer(), primary_key=True))
    nome: str = Field(default=None, nullable=True)
    email: str = Field(default=None, nullable=True)
    data_nascimento: date = Field(default=None, nullable=True)
    genero: str = Field(default=None, nullable=True)
    regiao: str = Field(default=None, nullable=True)
