"""FastAPI dependency: yields a SQLAlchemy session per request."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from lighthouse.config import load_config
from lighthouse.db.models import get_engine, init_db
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator

_config = load_config()
_engine = init_db(_config.database.url)
_SessionLocal = sessionmaker(bind=_engine)


def get_session() -> Generator[Session, None, None]:
    session = _SessionLocal()
    try:
        yield session
    finally:
        session.close()


def get_config():
    return _config
