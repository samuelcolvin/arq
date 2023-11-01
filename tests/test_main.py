import asyncio
import dataclasses
import functools
import logging
from collections import Counter
from datetime import datetime, timezone
from random import shuffle
from time import time

import msgpack
import pytest
from dirty_equals import IsInt, IsNow

from arq.connections import ArqRedis
from arq.constants import default_queue_name
from arq.jobs import Job, JobDef, SerializationError
from arq.utils import timestamp_ms
from arq.worker import Retry, Worker, func


async def test_enqueue_job(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    j = await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.main()
    r = await j.result(poll_delay=0)
    assert r == 42  # 1


async def test_enqueue_job_different_queues(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    j1 = await arq_redis.enqueue_job('foobar', _queue_name='arq:queue1')
    j2 = await arq_redis.enqueue_job('foobar', _queue_name='arq:queue2')
    worker1: Worker = worker(functions=[func(foobar, name='foobar')], queue_name='arq:queue1')
    worker2: Worker = worker(functions=[func(foobar, name='foobar')], queue_name='arq:queue2')

    await worker1.main()
    await worker2.main()
    r1 = await j1.result(poll_delay=0)
    r2 = await j2.result(poll_delay=0)
    assert r1 == 42  # 1
    assert r2 == 42  # 2


async def test_enqueue_job_nested(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    async def parent_job(ctx):
        inner_job = await ctx['redis'].enqueue_job('foobar')
        return inner_job.job_id

    job = await arq_redis.enqueue_job('parent_job')
    worker: Worker = worker(functions=[func(parent_job, name='parent_job'), func(foobar, name='foobar')])

    await worker.main()
    result = await job.result(poll_delay=0)
    assert result is not None
    inner_job = Job(result, arq_redis)
    inner_result = await inner_job.result(poll_delay=0)
    assert inner_result == 42


async def test_enqueue_job_nested_custom_serializer(arq_redis_msgpack: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    async def parent_job(ctx):
        inner_job = await ctx['redis'].enqueue_job('foobar')
        return inner_job.job_id

    job = await arq_redis_msgpack.enqueue_job('parent_job')

    worker: Worker = worker(
        functions=[func(parent_job, name='parent_job'), func(foobar, name='foobar')],
        arq_redis=None,
        job_serializer=msgpack.packb,
        job_deserializer=functools.partial(msgpack.unpackb, raw=False),
    )

    await worker.main()
    result = await job.result(poll_delay=0)
    assert result is not None
    inner_job = Job(result, arq_redis_msgpack, _deserializer=functools.partial(msgpack.unpackb, raw=False))
    inner_result = await inner_job.result(poll_delay=0)
    assert inner_result == 42


async def test_enqueue_job_custom_queue(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return 42

    async def parent_job(ctx):
        inner_job = await ctx['redis'].enqueue_job('foobar')
        return inner_job.job_id

    job = await arq_redis.enqueue_job('parent_job', _queue_name='spanner')

    worker: Worker = worker(
        functions=[func(parent_job, name='parent_job'), func(foobar, name='foobar')],
        arq_redis=None,
        queue_name='spanner',
    )

    await worker.main()
    inner_job_id = await job.result(poll_delay=0)
    assert inner_job_id is not None
    inner_job = Job(inner_job_id, arq_redis, _queue_name='spanner')
    inner_result = await inner_job.result(poll_delay=0)
    assert inner_result == 42


async def test_job_error(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        raise RuntimeError('foobar error')

    j = await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.main()

    with pytest.raises(RuntimeError, match='foobar error'):
        await j.result(poll_delay=0)


async def test_job_info(arq_redis: ArqRedis):
    t_before = time()
    j = await arq_redis.enqueue_job('foobar', 123, a=456)
    info = await j.info()
    assert info.enqueue_time == IsNow(tz='utc')
    assert info.job_try is None
    assert info.function == 'foobar'
    assert info.args == (123,)
    assert info.kwargs == {'a': 456}
    assert abs(t_before * 1000 - info.score) < 1000


async def test_repeat_job(arq_redis: ArqRedis):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id')
    assert isinstance(j1, Job)
    j2 = await arq_redis.enqueue_job('foobar', _job_id='job_id')
    assert j2 is None


async def test_defer_until(arq_redis: ArqRedis):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id', _defer_until=datetime(2032, 1, 1, tzinfo=timezone.utc))
    assert isinstance(j1, Job)
    score = await arq_redis.zscore(default_queue_name, 'job_id')
    assert score == 1_956_528_000_000


async def test_defer_by(arq_redis: ArqRedis):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id', _defer_by=20)
    assert isinstance(j1, Job)
    score = await arq_redis.zscore(default_queue_name, 'job_id')
    ts = timestamp_ms()
    assert score > ts + 19000
    assert ts + 21000 > score


async def test_mung(arq_redis: ArqRedis, worker):
    """
    check a job can't be enqueued multiple times with the same id
    """
    counter = Counter()

    async def count(ctx, v):
        counter[v] += 1

    tasks = []
    for i in range(50):
        tasks += [
            arq_redis.enqueue_job('count', i, _job_id=f'v-{i}'),
            arq_redis.enqueue_job('count', i, _job_id=f'v-{i}'),
        ]
    shuffle(tasks)
    await asyncio.gather(*tasks)

    worker: Worker = worker(functions=[func(count, name='count')])
    await worker.main()
    assert counter.most_common(1)[0][1] == 1  # no job go enqueued twice


async def test_custom_try(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        return ctx['job_try']

    j1 = await arq_redis.enqueue_job('foobar')
    w: Worker = worker(functions=[func(foobar, name='foobar')])
    await w.main()
    r = await j1.result(poll_delay=0)
    assert r == 1

    j2 = await arq_redis.enqueue_job('foobar', _job_try=3)
    await w.main()
    r = await j2.result(poll_delay=0)
    assert r == 3


async def test_custom_try2(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        if ctx['job_try'] == 3:
            raise Retry()
        return ctx['job_try']

    j1 = await arq_redis.enqueue_job('foobar', _job_try=3)
    w: Worker = worker(functions=[func(foobar, name='foobar')])
    await w.main()
    r = await j1.result(poll_delay=0)
    assert r == 4


async def test_cant_pickle_arg(arq_redis: ArqRedis):
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    with pytest.raises(SerializationError, match='unable to serialize job "foobar"'):
        await arq_redis.enqueue_job('foobar', Foobar())


async def test_cant_pickle_result(arq_redis: ArqRedis, worker):
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    async def foobar(ctx):
        return Foobar()

    j1 = await arq_redis.enqueue_job('foobar')
    w: Worker = worker(functions=[func(foobar, name='foobar')])
    await w.main()
    with pytest.raises(SerializationError, match='unable to serialize result'):
        await j1.result(poll_delay=0)


async def test_get_jobs(arq_redis: ArqRedis):
    await arq_redis.enqueue_job('foobar', a=1, b=2, c=3, _job_id='1')
    await asyncio.sleep(0.01)
    await arq_redis.enqueue_job('second', 4, b=5, c=6, _job_id='2')
    await asyncio.sleep(0.01)
    await arq_redis.enqueue_job('third', 7, b=8, _job_id='3')
    jobs = await arq_redis.queued_jobs()
    assert [dataclasses.asdict(j) for j in jobs] == [
        {
            'function': 'foobar',
            'args': (),
            'kwargs': {'a': 1, 'b': 2, 'c': 3},
            'job_try': None,
            'enqueue_time': IsNow(tz='utc'),
            'score': IsInt(),
            'job_id': '1',
        },
        {
            'function': 'second',
            'args': (4,),
            'kwargs': {'b': 5, 'c': 6},
            'job_try': None,
            'enqueue_time': IsNow(tz='utc'),
            'score': IsInt(),
            'job_id': '2',
        },
        {
            'function': 'third',
            'args': (7,),
            'kwargs': {'b': 8},
            'job_try': None,
            'enqueue_time': IsNow(tz='utc'),
            'score': IsInt(),
            'job_id': '3',
        },
    ]
    assert jobs[0].score < jobs[1].score < jobs[2].score
    assert isinstance(jobs[0], JobDef)
    assert isinstance(jobs[1], JobDef)
    assert isinstance(jobs[2], JobDef)


async def test_enqueue_multiple(arq_redis: ArqRedis, caplog):
    caplog.set_level(logging.DEBUG)
    results = await asyncio.gather(*[arq_redis.enqueue_job('foobar', i, _job_id='testing') for i in range(10)])
    assert sum(r is not None for r in results) == 1
    assert sum(r is None for r in results) == 9
    assert 'WatchVariableError' not in caplog.text
