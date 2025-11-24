FROM python:3.13-slim

WORKDIR /app

RUN pip install poetry

COPY poetry.lock pyproject.toml /app/

RUN poetry install --no-root

COPY ./src /app

CMD [ "poetry", "run", "python", "main.py" ]