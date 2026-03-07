import asyncio
from functools import wraps

def login_required(func):
    # In Airflow 2.x+, authentication is handled by FAB/security manager.
    # This decorator is a no-op passthrough; actual auth is via @has_access.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
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
