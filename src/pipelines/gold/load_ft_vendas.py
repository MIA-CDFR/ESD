from sqlmodel import select

from models.silver import MetodoPagamento, Produto, Venda, Utilizador
from models.gold import DimMetodoPagamento, DimProduto, DimProdutoCategoria, DimUtilizador, FtVendas

from utils.database import get_session, get_silver_session


def run_load() -> None:
    with next(get_silver_session()) as silver_session:
        silver_data = silver_session.exec(
            select(Venda)
        ).all()

        batch = []
        with next(get_session()) as session:
            for row in silver_data:
                utilizador = silver_session.get(Utilizador, row.utilizador_id)
                dim_utilizador = session.exec(
                    select(DimUtilizador).where(
                        DimUtilizador.nome==utilizador.nome,
                        DimUtilizador.data_nascimento==utilizador.data_nascimento,
                    )
                ).first()

                produto = silver_session.get(Produto, row.utilizador_id)
                dim_produto = session.exec(
                    select(DimProduto).where(
                        DimProduto.nome==produto.produto,
                    )
                ).first()

                dim_produto_categoria = session.exec(
                    select(DimProdutoCategoria).where(
                        DimProdutoCategoria.tipo==produto.regiao,
                    )
                ).first()

                metodo_pagamento = silver_session.get(MetodoPagamento, row.metodo_pagamento_id)
                dim_metodo_pagamento = session.exec(
                    select(DimMetodoPagamento).where(
                        DimMetodoPagamento.tipo_metodo==metodo_pagamento.metodo_pagamento,
                    )
                ).first()

                _existent = session.exec(
                    select(FtVendas).where(
                        FtVendas.venda_key==row.venda_id,
                        FtVendas.venda_date_key==int(row.datahora.strftime("%Y%m%d")),
                        FtVendas.utilizador_key==dim_utilizador.utilizador_key,
                        FtVendas.produto_key==dim_produto.produto_key,
                        FtVendas.produto_categoria_key==dim_produto_categoria.produto_categoria_key,
                        FtVendas.metodo_pagamento_key==dim_metodo_pagamento.metodo_pagamento_key,
                    )
                ).first()
                if _existent:
                    continue

                dim = FtVendas(
                    venda_key=row.venda_id,
                    venda_date_key=int(row.datahora.strftime("%Y%m%d")),
                    utilizador_key=dim_utilizador.utilizador_key,
                    produto_key=dim_produto.produto_key,
                    produto_categoria_key=dim_produto_categoria.produto_categoria_key,
                    metodo_pagamento_key=dim_metodo_pagamento.metodo_pagamento_key,
                    valor_venda=row.quantidade + row.valor_unitario,
                    valor_desconto=0,
                    valor_iva=(row.quantidade + row.valor_unitario) * 0.23,
                )

                batch.append(dim)

                if len(batch) >= 1000:
                    session.add_all(batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.add_all(batch)
                session.commit()
