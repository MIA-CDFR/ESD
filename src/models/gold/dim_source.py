from sqlmodel import Field, SQLModel


class DimSource(SQLModel, table=True):

    __tablename__ = "dim_source"

    source_key: int = Field(default=None, primary_key=True, nullable=False)
    nome: str = Field(default=None, nullable=False)
