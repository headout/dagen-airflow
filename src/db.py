"""
Direct database access for Airflow 3.x task subprocesses.

Airflow 3.x blocks direct ORM access via Session() in task subprocesses.
This module provides a direct SQLAlchemy session using the same DB connection
string, bypassing Airflow's restriction for custom tables (ergo_task, ergo_job).
"""
import functools
import logging
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

_engine = None
_SessionFactory = None


def _get_engine():
    global _engine
    if _engine is None:
        from airflow.configuration import conf
        sql_alchemy_conn = conf.get("database", "SQL_ALCHEMY_CONN")
        # URL-encode special characters in password (e.g. $ signs)
        from urllib.parse import quote
        parts = sql_alchemy_conn.split("@", 1)
        if "@" in sql_alchemy_conn and ":" in parts[0]:
            scheme_user, password = parts[0].rsplit(":", 1)
            password = quote(password, safe="")
            sql_alchemy_conn = f"{scheme_user}:{password}@{parts[1]}"
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
