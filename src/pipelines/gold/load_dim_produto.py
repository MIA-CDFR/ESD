from sqlmodel import select

from models.silver import Produto
from models.gold import DimProduto

from utils.database import get_session, get_silver_session


def run_load() -> None:
    with next(get_silver_session()) as silver_session:
        silver_data = silver_session.exec(
            select(Produto)
        ).all()

        batch = []
        with next(get_session()) as session:
            for row in silver_data:
                _existent = session.exec(
                    select(DimProduto).where(
                        DimProduto.nome==row.produto
                    )
                ).first()
                if _existent:
                    continue

                dim = DimProduto(
                    nome=row.produto,
                    categoria="NA",
                    safra=row.safra,
                    regiao=row.regiao,
                )

                batch.append(dim)

                if len(batch) >= 1000:
                    session.add_all(batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.add_all(batch)
                session.commit()
