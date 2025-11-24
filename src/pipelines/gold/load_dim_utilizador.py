from sqlmodel import select

from models.silver import Utilizador
from models.gold import DimUtilizador

from utils.database import get_session, get_silver_session


def run_load() -> None:
    with next(get_silver_session()) as silver_session:
        silver_data = silver_session.exec(
            select(Utilizador)
        ).all()

        batch = []
        with next(get_session()) as session:
            for row in silver_data:
                _existent = session.exec(
                    select(DimUtilizador).where(
                        DimUtilizador.nome==row.nome,
                        DimUtilizador.data_nascimento==row.data_nascimento,
                    )
                ).first()
                if _existent:
                    continue

                dim = DimUtilizador(
                    nome=row.nome,
                    regiao=row.regiao,
                    data_nascimento=row.data_nascimento,
                    sexo=row.genero,
                )

                batch.append(dim)

                if len(batch) >= 1000:
                    session.add_all(batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.add_all(batch)
                session.commit()
