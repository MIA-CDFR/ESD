from sqlmodel import Field, SQLModel


class DimProdutoCategoria(SQLModel, table=True):

    __tablename__ = "dim_produto_categoria"

    produto_categoria_key: int = Field(default=None, primary_key=True, nullable=False)
    tipo: str = Field(default=None, nullable=False)
