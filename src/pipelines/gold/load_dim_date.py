from datetime import date, timedelta

from models.gold import DimDate
from utils.database import get_session

def run_load(
    start_date: date = date(2010, 1, 1),
    end_date: date = date(2099, 12, 31)
) -> None:
    current_date = start_date
    batch = []

    with next(get_session()) as session:
        while current_date <= end_date:
            date_key = int(current_date.strftime("%Y%m%d"))

            if session.get(DimDate, date_key):
                current_date += timedelta(days=1)
                continue

            dim = DimDate(
                date_key=date_key,
                dia=current_date.day,
                dia_da_semana=current_date.isoweekday(),
                mes=current_date.month,
                nome_do_mes=current_date.strftime("%B"),
                semestre=(current_date.month - 1) // 3 + 1,
                ano=current_date.year,
                flag_feriado=False,
                flag_fim_de_semana=current_date.weekday() >= 5,
                flag_dia_de_semana=current_date.weekday() < 5
            )

            batch.append(dim)

            if len(batch) >= 1000:
                session.add_all(batch)
                session.commit()
                batch.clear()
                print(f"Inserido até {current_date}")

            current_date += timedelta(days=1)

        if batch:
            session.add_all(batch)
            session.commit()
