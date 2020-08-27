import asyncio
from functools import wraps

import airflow


def login_required(func):
    # when airflow loads plugins, login is still None.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if airflow.login:
            return airflow.login.login_required(func)(*args, **kwargs)
        return func(*args, **kwargs)
    return func_wrapper


# Credits: https://gist.github.com/privatwolke/11711cc26a843784afd1aeeb16308a30
async def async_gather_dict(tasks: dict):
    async def mark(key, coro):
        return key, await coro

    return {
        key: result
        for key, result in await asyncio.gather(
            *(mark(key, coro) for key, coro in tasks.items())
        )
    }
