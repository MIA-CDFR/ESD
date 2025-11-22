from typing import Generator
from sqlmodel import create_engine, Session

from utils import config


engine = create_engine(
    config.get_url("gold"),
    echo=True
)

silver_engine = create_engine(
    config.get_url("silver"),
    echo=True
)

def get_silver_session() -> Generator[Session, None, None]:
    session = Session(silver_engine)
    try:
        yield session
    finally:
        session.close()

def get_session() -> Generator[Session, None, None]:
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()
