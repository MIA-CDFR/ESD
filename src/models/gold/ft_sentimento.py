from sqlmodel import Field, SQLModel

from models.gold import DimDate, DimTipoSentimento, DimSource, DimUtilizador


class FtSentimento(SQLModel, table=True):

    __tablename__ = "ft_sentimento"

    sentimento_key: int = Field(default=None, primary_key=True, nullable=False)
    sentimento_date_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimDate.__tablename__}.date_key")
    utilizador_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimUtilizador.__tablename__}.utilizador_key")
    tipo_sentimento_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimTipoSentimento.__tablename__}.tipo_sentimento_key")
    source_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimSource.__tablename__}.source_key")
    score: float = Field(default=None, nullable=False)
