from sqlmodel import Field, SQLModel

from models.gold import DimDate, DimMetodoPagamento, DimProduto, DimRegiao, DimUtilizador


class FtVendas(SQLModel, table=True):

    __tablename__ = "ft_vendas"

    venda_key: int = Field(default=None, primary_key=True, nullable=False)
    venda_date_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimDate.__tablename__}.date_key")
    utilizador_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimUtilizador.__tablename__}.utilizador_key")
    produto_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimProduto.__tablename__}.produto_key")
    produto_categoria_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimRegiao.__tablename__}.regiao_key")
    metodo_pagamento_key: int = Field(default=None, primary_key=True, nullable=False, foreign_key=f"{DimMetodoPagamento.__tablename__}.metodo_pagamento_key")
    valor_venda: float = Field(default=None, nullable=False)
    valor_desconto: float = Field(default=None, nullable=False)
    valor_iva: float = Field(default=None, nullable=False)
