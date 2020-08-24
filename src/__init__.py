from functools import wraps

from airflow.utils import db

SECTION_NAME = 'dagen'


def dagen_initdb(func):
    from dagen.migrations.utils import initdb

    prev_wrappers = getattr(func, '_wrappers', list())
    if SECTION_NAME in prev_wrappers:
        return func

    @wraps(func)
    def wrapper(*args, **kwargs):
        func(*args, **kwargs)
        initdb()

    wrapper._wrappers = list(prev_wrappers) + list(SECTION_NAME)

    return wrapper


db.initdb = dagen_initdb(db.initdb)
