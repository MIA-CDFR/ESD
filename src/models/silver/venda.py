from datetime import datetime

from sqlalchemy import Integer
from sqlmodel import Column, Field, SQLModel


class Venda(SQLModel, table=True):

    __tablename__ = "venda"

    venda_id: int = Field(sa_column=Column(Integer(), primary_key=True))
    utilizador_id: int = Field(default=None, nullable=True)
    datahora: datetime = Field(default=None, nullable=True)
    loja_id: int = Field(default=None, nullable=True)
    produto_id: int = Field(default=None, nullable=True)
    quantidade: int = Field(default=None, nullable=True)
    valor_unitario: float = Field(default=None, nullable=True)
    metodo_pagamento_id: int = Field(default=None, nullable=True)
    comentario: str = Field(default=None, nullable=True)
    extracted: bool = Field(default=None, nullable=True)
