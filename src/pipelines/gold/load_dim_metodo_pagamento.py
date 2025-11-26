from sqlmodel import select

from models.silver import MetodoPagamento
from models.gold import DimMetodoPagamento

from utils.database import get_session, get_silver_session


def run_load() -> None:
    with next(get_silver_session()) as silver_session:
        silver_data = silver_session.exec(
            select(MetodoPagamento)
        ).all()

        batch = []
        with next(get_session()) as session:
            for row in silver_data:
                _existent = session.exec(
                    select(DimMetodoPagamento).where(
                        DimMetodoPagamento.tipo_metodo==row.metodo_pagamento
                    )
                ).first()
                if _existent:
                    continue

                dim = DimMetodoPagamento(
                    tipo_metodo=row.metodo_pagamento,
                )

                batch.append(dim)

                if len(batch) >= 1000:
                    session.add_all(batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.add_all(batch)
                session.commit()
