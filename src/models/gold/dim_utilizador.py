from datetime import date
from sqlmodel import Field, SQLModel


class DimUtilizador(SQLModel, table=True):

    __tablename__ = "dim_utilizador"

    utilizador_key: int = Field(default=None, primary_key=True, nullable=False)
    nome: str = Field(default=None, nullable=False)
    regiao: str = Field(default=None, nullable=True)
    data_nascimento: date = Field(default=None, nullable=False)
    sexo: str = Field(default=None, nullable=False)
