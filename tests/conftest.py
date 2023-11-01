import asyncio
import functools
import os
import sys

import msgpack
import pytest
import redis.exceptions
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redislite import Redis

from arq.connections import ArqRedis, create_pool
from arq.worker import Worker


@pytest.fixture(name='loop')
def _fix_loop(event_loop):
    return event_loop


@pytest.fixture
async def arq_redis(loop):
    redis_ = ArqRedis(
        host='localhost',
        port=6379,
        encoding='utf-8',
    )

    await redis_.flushall()

    yield redis_

    await redis_.close(close_connection_pool=True)


@pytest.fixture
async def unix_socket_path(loop, tmp_path):
    rdb = Redis(str(tmp_path / 'redis_test.db'))
    yield rdb.socket_file
    rdb.close()


@pytest.fixture
async def arq_redis_msgpack(loop):
    redis_ = ArqRedis(
        host='localhost',
        port=6379,
        encoding='utf-8',
        job_serializer=msgpack.packb,
        job_deserializer=functools.partial(msgpack.unpackb, raw=False),
    )
    await redis_.flushall()
    yield redis_
    await redis_.close(close_connection_pool=True)


@pytest.fixture
async def arq_redis_retry(loop):
    redis_ = ArqRedis(
        host='localhost',
        port=6379,
        encoding='utf-8',
        retry=Retry(backoff=NoBackoff(), retries=3),
        retry_on_timeout=True,
        retry_on_error=[redis.exceptions.ConnectionError],
    )
    await redis_.flushall()
    yield redis_
    await redis_.close(close_connection_pool=True)


@pytest.fixture
async def worker(arq_redis):
    worker_: Worker = None

    def create(functions=[], burst=True, poll_delay=0, max_jobs=10, arq_redis=arq_redis, **kwargs):
        nonlocal worker_
        worker_ = Worker(
            functions=functions, redis_pool=arq_redis, burst=burst, poll_delay=poll_delay, max_jobs=max_jobs, **kwargs
        )
        return worker_

    yield create

    if worker_:
        await worker_.close()


@pytest.fixture
async def worker_retry(arq_redis_retry):
    worker_retry_: Worker = None

    def create(functions=[], burst=True, poll_delay=0, max_jobs=10, arq_redis=arq_redis_retry, **kwargs):
        nonlocal worker_retry_
        worker_retry_ = Worker(
            functions=functions,
            redis_pool=arq_redis,
            burst=burst,
            poll_delay=poll_delay,
            max_jobs=max_jobs,
            **kwargs,
        )
        return worker_retry_

    yield create

    if worker_retry_:
        await worker_retry_.close()


@pytest.fixture(name='create_pool')
async def fix_create_pool(loop):
    pools = []

    async def create_pool_(settings, *args, **kwargs):
        pool = await create_pool(settings, *args, **kwargs)
        pools.append(pool)
        return pool

    yield create_pool_

    await asyncio.gather(*[p.close(close_connection_pool=True) for p in pools])


@pytest.fixture(name='cancel_remaining_task')
def fix_cancel_remaining_task(loop):
    async def cancel_remaining_task():
        tasks = asyncio.all_tasks(loop)
        cancelled = []
        for task in tasks:
            # in repr works in 3.7 where get_coro() is not available
            if 'cancel_remaining_task()' not in repr(task):
                cancelled.append(task)
                task.cancel()
        if cancelled:
            print(f'Cancelled {len(cancelled)} ongoing tasks', file=sys.stderr)
            await asyncio.gather(*cancelled, return_exceptions=True)

    yield

    loop.run_until_complete(cancel_remaining_task())


class SetEnv:
    def __init__(self):
        self.envars = set()

    def set(self, name, value):
        self.envars.add(name)
        os.environ[name] = value

    def clear(self):
        for n in self.envars:
            os.environ.pop(n)


@pytest.fixture
def env():
    setenv = SetEnv()

    yield setenv

    setenv.clear()
