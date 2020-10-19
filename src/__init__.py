import logging
from functools import wraps

from airflow.utils import db

SECTION_NAME = 'dagen'

logger = logging.getLogger(__name__)


def dagen_initdb(func):
    from dagen.migrations.utils import initdb

    prev_wrappers = getattr(func, '_wrappers', list())
    if SECTION_NAME in prev_wrappers:
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            logger.warning('Ignoring error', exc_info=e)
        initdb()

    wrapper._wrappers = list(prev_wrappers) + list(SECTION_NAME)

    return wrapper


db.upgradedb = dagen_initdb(db.upgradedb)
