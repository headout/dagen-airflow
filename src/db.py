"""
Direct database access for Airflow 3.x task subprocesses.

Airflow 3.x blocks direct ORM access via Session() in task subprocesses.
block_orm_access() overwrites env vars and conf with "airflow-db-not-allowed:///".
We capture the real DB URL at import time (before block_orm_access runs) so our
custom tables (dagen_dag, ergo_task, ergo_job) can still be accessed.
"""
import functools
import logging
import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Capture at import time — before Airflow's block_orm_access() overwrites them.
_DB_URL = (
    os.environ.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN")
    or os.environ.get("AIRFLOW__CORE__SQL_ALCHEMY_CONN")
)

_engine = None
_SessionFactory = None


def _get_engine():
    global _engine
    if _engine is None:
        sql_alchemy_conn = _DB_URL
        if not sql_alchemy_conn:
            from airflow.configuration import conf
            sql_alchemy_conn = conf.get("database", "SQL_ALCHEMY_CONN")
        _engine = create_engine(sql_alchemy_conn, pool_pre_ping=True)
    return _engine


def _get_session_factory():
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=_get_engine())
    return _SessionFactory


@contextmanager
def create_session():
    """Create a direct DB session, bypassing Airflow's blocked Session()."""
    session = _get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def provide_session(func):
    """Drop-in replacement for airflow.utils.session.provide_session.

    Uses a direct SQLAlchemy session instead of Airflow's blocked Session().
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'session' not in kwargs or kwargs['session'] is None:
            with create_session() as session:
                kwargs['session'] = session
                return func(*args, **kwargs)
        return func(*args, **kwargs)
    return wrapper
