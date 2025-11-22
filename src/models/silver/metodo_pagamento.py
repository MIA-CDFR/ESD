from sqlalchemy import Integer
from sqlmodel import Column, Field, SQLModel


class MetodoPagamento(SQLModel, table=True):

    __tablename__ = "metodo_pagamento"

    metodo_pagamento_id: int = Field(sa_column=Column(Integer(), primary_key=True))
    metodo_pagamento: str = Field(default=None, nullable=True)
