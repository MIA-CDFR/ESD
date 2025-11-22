from sqlmodel import Field, SQLModel


class DimMetodoPagamento(SQLModel, table=True):

    __tablename__ = "dim_metodo_pagamento"

    metodo_pagamento_key: int = Field(default=None, primary_key=True, nullable=False)
    tipo_metodo: str = Field(default=None, nullable=False)
