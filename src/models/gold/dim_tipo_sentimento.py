from sqlmodel import Field, SQLModel


class DimTipoSentimento(SQLModel, table=True):

    __tablename__ = "dim_tipo_sentimento"

    tipo_sentimento_key: int = Field(default=None, primary_key=True, nullable=False)
    sentimento: str = Field(default=None, nullable=False)
    classificacao: str = Field(default=None, nullable=True)
