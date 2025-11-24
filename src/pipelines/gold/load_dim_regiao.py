from sqlmodel import select

from models.silver import Produto
from models.gold import DimRegiao

from utils.database import get_session, get_silver_session


def run_load() -> None:
    with next(get_silver_session()) as silver_session:
        silver_data = silver_session.exec(
            select(Produto).distinct(Produto.regiao)
        ).all()

        batch = []
        with next(get_session()) as session:
            for row in silver_data:
                _existent = session.exec(
                    select(DimRegiao).where(
                        DimRegiao.nome_regiao==row.regiao
                    )
                ).first()
                if _existent:
                    continue

                dim = DimRegiao(
                    nome_regiao=row.regiao,
                )

                batch.append(dim)

                if len(batch) >= 1000:
                    session.add_all(batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.add_all(batch)
                session.commit()
