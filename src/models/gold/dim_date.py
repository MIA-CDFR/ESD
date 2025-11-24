from sqlmodel import Field, SQLModel


class DimDate(SQLModel, table=True):

    __tablename__ = "dim_date"

    date_key: int = Field(default=None, primary_key=True, nullable=False)
    dia: int = Field(default=None, nullable=False)
    dia_da_semana: int = Field(default=None, nullable=False)
    mes: int = Field(default=None, nullable=False)
    nome_do_mes: str = Field(default=None, nullable=False)
    semestre: int = Field(default=None, nullable=False)
    ano: int = Field(default=None, nullable=False)
    flag_feriado: bool = Field(default=None, nullable=False)
    flag_fim_de_semana: bool = Field(default=None, nullable=False)
    flag_dia_de_semana: bool = Field(default=None, nullable=False)
