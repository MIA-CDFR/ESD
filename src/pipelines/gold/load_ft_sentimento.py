from sqlmodel import select

from models.silver import Sentimento, Utilizador
from models.gold import DimSource, DimTipoSentimento, DimUtilizador, FtSentimento

from utils.database import get_session, get_silver_session


def run_load() -> None:
    with next(get_silver_session()) as silver_session:
        silver_data = silver_session.exec(
            select(Sentimento)
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

                dim_tipo_sentimento = session.exec(
                    select(DimTipoSentimento).where(
                        DimTipoSentimento.sentimento==row.sentimento,
                    )
                ).first()

                dim_source = session.exec(
                    select(DimSource).where(
                        DimSource.nome=="website feedback",
                    )
                ).first()

                _existent = session.exec(
                    select(FtSentimento).where(
                        FtSentimento.sentimento_key==row.sentimento_id,
                        FtSentimento.sentimento_date_key==int(row.datahora.strftime("%Y%m%d")),
                        FtSentimento.utilizador_key==dim_utilizador.utilizador_key,
                        FtSentimento.tipo_sentimento_key==dim_tipo_sentimento.tipo_sentimento_key,
                        FtSentimento.source_key==dim_source.source_key,
                    )
                ).first()
                if _existent:
                    continue

                dim = FtSentimento(
                    sentimento_key=row.sentimento_id,
                    sentimento_date_key=int(row.datahora.strftime("%Y%m%d")),
                    utilizador_key=dim_utilizador.utilizador_key,
                    tipo_sentimento_key=dim_tipo_sentimento.tipo_sentimento_key,
                    source_key=dim_source.source_key,
                    score=row.score,
                )

                batch.append(dim)

                if len(batch) >= 1000:
                    session.add_all(batch)
                    session.commit()
                    batch.clear()

            if batch:
                session.add_all(batch)
                session.commit()
