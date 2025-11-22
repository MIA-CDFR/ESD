from sqlmodel import Field, SQLModel


class DimRegiao(SQLModel, table=True):

    __tablename__ = "dim_regiao"

    regiao_key: int = Field(default=None, primary_key=True, nullable=False)
    nome_regiao: str = Field(default=None, nullable=False)
