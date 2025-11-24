from sqlmodel import Field, SQLModel


class DimProduto(SQLModel, table=True):

    __tablename__ = "dim_produto"

    produto_key: int = Field(default=None, primary_key=True, nullable=False)
    nome: str = Field(default=None, nullable=False)
    categoria: str = Field(default=None, nullable=True)
    safra: str = Field(default=None, nullable=True)
    regiao: str = Field(default=None, nullable=True)
