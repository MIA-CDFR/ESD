from sqlmodel import select

from models.silver import WebsiteFeedback
from models.gold import DimSource

from utils.database import get_session, get_silver_session


def run_load() -> None:
    with next(get_silver_session()) as silver_session:
        silver_data = silver_session.exec(
            select(WebsiteFeedback).distinct(WebsiteFeedback.source)
        ).all()

        batch = []
        with next(get_session()) as session:
            for row in silver_data:
                _existent = session.exec(
                    select(DimSource).where(
                        DimSource.nome==row.source
                    )
                ).first()
                if _existent:
                    continue

                dim = DimSource(
                    nome=row.source,
                )

                batch.append(dim)

                if len(batch) >= 1000:
                    session.add_all(batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.add_all(batch)
                session.commit()
