import asyncio
import pickle

import pytest
from dirty_equals import IsNow, IsStr

from arq import Worker, func
from arq.connections import ArqRedis, RedisSettings, create_pool
from arq.constants import default_queue_name, in_progress_key_prefix, job_key_prefix, result_key_prefix
from arq.jobs import (
    DeserializationError,
    Job,
    JobResult,
    JobStatus,
    ResultNotFound,
    deserialize_job_raw,
    serialize_result,
)


async def test_job_in_progress(arq_redis: ArqRedis):
    await arq_redis.set(in_progress_key_prefix + 'foobar', b'1')
    j = Job('foobar', arq_redis)
    assert JobStatus.in_progress == await j.status()
    assert str(j) == '<arq job foobar>'


async def test_unknown(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    assert JobStatus.not_found == await j.status()
    info = await j.info()
    assert info is None


async def test_result_timeout(arq_redis: ArqRedis):
    j = await arq_redis.enqueue_job('foobar')
    with pytest.raises(asyncio.TimeoutError):
        await j.result(0.1, poll_delay=0)


async def test_result_not_found(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    with pytest.raises(ResultNotFound):
        await j.result()


async def test_result_when_job_does_not_keep_result(arq_redis: ArqRedis, worker):
    async def foobar(ctx):
        pass

    worker: Worker = worker(functions=[func(foobar, name='foobar', keep_result=0)])
    j = await arq_redis.enqueue_job('foobar')

    result_call = asyncio.Task(j.result())

    _, pending = await asyncio.wait([result_call], timeout=0.1)
    assert pending == {result_call}

    await worker.main()

    with pytest.raises(ResultNotFound):
        # Job has completed and did not store any result
        await asyncio.wait_for(result_call, timeout=5)


async def test_enqueue_job(arq_redis: ArqRedis, worker, queue_name=default_queue_name):
    async def foobar(ctx, *args, **kwargs):
        return 42

    j = await arq_redis.enqueue_job('foobar', 1, 2, c=3, _queue_name=queue_name)
    assert isinstance(j, Job)
    assert JobStatus.queued == await j.status()
    worker: Worker = worker(functions=[func(foobar, name='foobar')], queue_name=queue_name)
    await worker.main()
    r = await j.result(poll_delay=0)
    assert r == 42
    assert JobStatus.complete == await j.status()
    info = await j.info()
    expected_queue_name = queue_name or arq_redis.default_queue_name
    assert info == JobResult(
        job_try=1,
        function='foobar',
        args=(1, 2),
        kwargs={'c': 3},
        enqueue_time=IsNow(tz='utc'),
        success=True,
        result=42,
        start_time=IsNow(tz='utc'),
        finish_time=IsNow(tz='utc'),
        score=None,
        queue_name=expected_queue_name,
        job_id=IsStr(),
    )
    results = await arq_redis.all_job_results()
    assert results == [
        JobResult(
            function='foobar',
            args=(1, 2),
            kwargs={'c': 3},
            job_try=1,
            enqueue_time=IsNow(tz='utc'),
            success=True,
            result=42,
            start_time=IsNow(tz='utc'),
            finish_time=IsNow(tz='utc'),
            score=None,
            queue_name=expected_queue_name,
            job_id=j.job_id,
        )
    ]


async def test_enqueue_job_with_unix_socket(worker, unix_socket_path):
    """Test initializing arq_redis using a unix socket connection, and the worker using it."""
    settings = RedisSettings(unix_socket_path=unix_socket_path)
    arq_redis = await create_pool(settings, default_queue_name='socket_queue')
    await test_enqueue_job(
        arq_redis,
        lambda functions, **_: worker(functions=functions, arq_redis=arq_redis, queue_name=None),
        queue_name=None,
    )


async def test_enqueue_job_alt_queue(arq_redis: ArqRedis, worker):
    await test_enqueue_job(arq_redis, worker, queue_name='custom_queue')


async def test_enqueue_job_nondefault_queue(worker):
    """Test initializing arq_redis with a queue name, and the worker using it."""
    arq_redis = await create_pool(RedisSettings(), default_queue_name='test_queue')
    await test_enqueue_job(
        arq_redis,
        lambda functions, **_: worker(functions=functions, arq_redis=arq_redis, queue_name=None),
        queue_name=None,
    )


async def test_cant_unpickle_at_all():
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    r1 = serialize_result('foobar', (1,), {}, 1, 123, True, Foobar(), 123, 123, 'testing', 'test-queue', 'job_1')
    assert isinstance(r1, bytes)
    r2 = serialize_result('foobar', (Foobar(),), {}, 1, 123, True, Foobar(), 123, 123, 'testing', 'test-queue', 'job_1')
    assert r2 is None


async def test_custom_serializer():
    class Foobar:
        def __getstate__(self):
            raise TypeError("this doesn't pickle")

    def custom_serializer(x):
        return b'0123456789'

    r1 = serialize_result(
        'foobar',
        (1,),
        {},
        1,
        123,
        True,
        Foobar(),
        123,
        123,
        'testing',
        'test-queue',
        'job_1',
        serializer=custom_serializer,
    )
    assert r1 == b'0123456789'
    r2 = serialize_result(
        'foobar',
        (Foobar(),),
        {},
        1,
        123,
        True,
        Foobar(),
        123,
        123,
        'testing',
        'test-queue',
        'job_1',
        serializer=custom_serializer,
    )
    assert r2 == b'0123456789'


async def test_deserialize_result(arq_redis: ArqRedis, worker):
    async def foobar(ctx, a, b):
        return a + b

    j = await arq_redis.enqueue_job('foobar', 1, 2)
    assert JobStatus.queued == await j.status()
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    await worker.run_check()
    assert await j.result(poll_delay=0) == 3
    assert await j.result(poll_delay=0) == 3
    info = await j.info()
    assert info.args == (1, 2)
    await arq_redis.set(result_key_prefix + j.job_id, b'invalid pickle data')
    with pytest.raises(DeserializationError, match='unable to deserialize job result'):
        assert await j.result(poll_delay=0) == 3


async def test_deserialize_info(arq_redis: ArqRedis):
    j = await arq_redis.enqueue_job('foobar', 1, 2)
    assert JobStatus.queued == await j.status()
    await arq_redis.set(job_key_prefix + j.job_id, b'invalid pickle data')

    with pytest.raises(DeserializationError, match='unable to deserialize job'):
        assert await j.info()


async def test_deserialize_job_raw():
    assert deserialize_job_raw(pickle.dumps({'f': 1, 'a': 2, 'k': 3, 't': 4, 'et': 5})) == (1, 2, 3, 4, 5)
    with pytest.raises(DeserializationError, match='unable to deserialize job'):
        deserialize_job_raw(b'123')


async def test_get_job_result(arq_redis: ArqRedis):
    with pytest.raises(KeyError, match='job "foobar" not found'):
        await arq_redis._get_job_result(b'foobar')


async def test_result_pole_delay_dep(arq_redis: ArqRedis):
    j = Job('foobar', arq_redis)
    r = serialize_result('foobar', (1,), {}, 1, 123, True, 42, 123, 123, 'testing', 'test-queue', 'job_1')
    await arq_redis.set(result_key_prefix + j.job_id, r)
    with pytest.warns(
        DeprecationWarning, match='"pole_delay" is deprecated, use the correct spelling "poll_delay" instead'
    ):
        assert await j.result(pole_delay=0) == 42
