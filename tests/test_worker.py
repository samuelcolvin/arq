import asyncio
import functools
import logging
import re
import signal
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import msgpack
import pytest
import redis.exceptions

from arq.connections import ArqRedis, RedisSettings
from arq.constants import abort_jobs_ss, default_queue_name, expires_extra_ms, health_check_key_suffix, job_key_prefix
from arq.jobs import Job, JobStatus
from arq.worker import (
    FailedJobs,
    JobExecutionFailed,
    Retry,
    RetryJob,
    Worker,
    async_check_health,
    check_health,
    func,
    run_worker,
)


async def foobar(ctx):
    return 42


async def fails(ctx):
    raise TypeError('my type error')


def test_no_jobs(arq_redis: ArqRedis, loop, mocker):
    class Settings:
        functions = [func(foobar, name='foobar')]
        burst = True
        poll_delay = 0
        queue_read_limit = 10

    loop.run_until_complete(arq_redis.enqueue_job('foobar'))
    mocker.patch('asyncio.get_event_loop', lambda: loop)
    worker = run_worker(Settings)
    assert worker.jobs_complete == 1
    assert str(worker) == '<Worker j_complete=1 j_failed=0 j_retried=0 j_ongoing=0>'


def test_health_check_direct(loop):
    class Settings:
        pass

    asyncio.set_event_loop(loop)
    assert check_health(Settings) == 1


async def test_health_check_fails():
    assert 1 == await async_check_health(None)


async def test_health_check_pass(arq_redis):
    await arq_redis.set(default_queue_name + health_check_key_suffix, b'1')
    assert 0 == await async_check_health(None)


async def test_set_health_check_key(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[func(foobar, keep_result=0)], health_check_key='arq:test:health-check')
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == [b'arq:test:health-check']


async def test_handle_sig(caplog, arq_redis: ArqRedis):
    caplog.set_level(logging.INFO)
    worker = Worker([foobar], redis_pool=arq_redis)
    worker.main_task = MagicMock()
    worker.tasks = {0: MagicMock(done=MagicMock(return_value=True)), 1: MagicMock(done=MagicMock(return_value=False))}

    assert len(caplog.records) == 0
    worker.handle_sig(signal.SIGINT)
    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        'shutdown on SIGINT ◆ 0 jobs complete ◆ 0 failed ◆ 0 retries ◆ 2 ongoing to cancel'
    )
    assert worker.main_task.cancel.call_count == 1
    assert worker.tasks[0].done.call_count == 1
    assert worker.tasks[0].cancel.call_count == 0
    assert worker.tasks[1].done.call_count == 1
    assert worker.tasks[1].cancel.call_count == 1


async def test_handle_no_sig(caplog):
    caplog.set_level(logging.INFO)
    worker = Worker([foobar], handle_signals=False)
    worker.main_task = MagicMock()
    worker.tasks = {0: MagicMock(done=MagicMock(return_value=True)), 1: MagicMock(done=MagicMock(return_value=False))}

    assert len(caplog.records) == 0
    await worker.close()
    assert len(caplog.records) == 1
    assert caplog.records[0].message == (
        'shutdown on SIGUSR1 ◆ 0 jobs complete ◆ 0 failed ◆ 0 retries ◆ 2 ongoing to cancel'
    )
    assert worker.main_task.cancel.call_count == 1
    assert worker.tasks[0].done.call_count == 1
    assert worker.tasks[0].cancel.call_count == 0
    assert worker.tasks[1].done.call_count == 1
    assert worker.tasks[1].cancel.call_count == 1


async def test_worker_signal_completes_job_before_shutting_down(caplog, arq_redis: ArqRedis, worker):
    caplog.set_level(logging.INFO)

    async def sleep_job(ctx, time):
        await asyncio.sleep(time)

    await arq_redis.enqueue_job('sleep_job', 0.2, _job_id='short_sleep')  # should be completed
    await arq_redis.enqueue_job('sleep_job', 5, _job_id='long_sleep')  # should be cancelled
    worker = worker(
        functions=[func(sleep_job, name='sleep_job', max_tries=1)],
        job_completion_wait=0.5,
        job_timeout=10,
    )
    assert worker.jobs_complete == 0
    asyncio.create_task(worker.main())
    await asyncio.sleep(0.1)
    worker.handle_sig_wait_for_completion(signal.SIGINT)
    assert len(worker.tasks) == 2  # should be two tasks when sigint is sent
    assert worker.allow_pick_jobs is False
    await asyncio.sleep(0.3)
    assert len(worker.tasks) == 1  # slept a bit, first job should now be complete and self.tasks should be updated
    await asyncio.sleep(0.3)
    assert len(worker.tasks) == 0  # slept longer than `job_completion_wait`, task should be cancelled and updated
    logs = [rec.message for rec in caplog.records]
    assert 'shutdown on SIGINT ◆ 0 jobs complete ◆ 0 failed ◆ 0 retries ◆ 2 to be completed' in logs
    assert 'shutdown on SIGINT, wait complete ◆ 1 jobs complete ◆ 0 failed ◆ 0 retries ◆ 1 ongoing to cancel' in logs
    assert 'long_sleep:sleep_job cancelled, will be run again' in logs[-1]
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 1
    assert worker.jobs_failed == 0


async def test_job_successful(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs → testing:foobar()\n  X.XXs ← testing:foobar ● 42' in log


async def test_job_successful_no_result_logging(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar], log_results=False)
    await worker.main()

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert log.endswith('X.XXs → testing:foobar()\n  X.XXs ← testing:foobar ● ')
    assert '42' not in log


async def test_job_retry(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        if ctx['job_try'] <= 2:
            raise Retry(defer=0.01)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 2

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '0.XXs ↻ testing:retry retrying job in 0.XXs\n' in log
    assert '0.XXs → testing:retry() try=2\n' in log
    assert '0.XXs ← testing:retry ●' in log


async def test_job_retry_dont_retry(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry(defer=0.01)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check(retry_jobs=False)
    assert str(exc_info.value) == '1 job failed <Retry defer 0.01s>'

    assert '↻' not in caplog.text
    assert '! testing:retry failed, Retry: <Retry defer 0.01s>\n' in caplog.text


async def test_job_retry_max_jobs(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry(defer=0.01)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    assert await worker.run_check(max_burst_jobs=1) == 0
    assert worker.jobs_complete == 0
    assert worker.jobs_retried == 1
    assert worker.jobs_failed == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', caplog.text)
    assert '0.XXs ↻ testing:retry retrying job in 0.XXs\n' in log
    assert '0.XXs → testing:retry() try=2\n' not in log


async def test_job_job_not_found(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('missing', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert "job testing, function 'missing' not found" in log


async def test_job_job_not_found_run_check(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('missing', _job_id='testing')
    worker: Worker = worker(functions=[foobar])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()

    assert exc_info.value.count == 1
    assert len(exc_info.value.job_results) == 1
    failure = exc_info.value.job_results[0].result
    assert failure == JobExecutionFailed("function 'missing' not found")
    assert failure != 123  # check the __eq__ method of JobExecutionFailed


async def test_retry_lots(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 5

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert '  X.XXs ! testing:retry max retries 5 exceeded' in log


async def test_retry_lots_without_keep_result(arq_redis: ArqRedis, worker):
    async def retry(ctx):
        raise Retry()

    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')], keep_result=0)
    await worker.main()  # Should not raise MultiExecError


async def test_retry_lots_check(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        raise Retry()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    with pytest.raises(FailedJobs, match='max 5 retries exceeded'):
        await worker.run_check()


@pytest.mark.skipif(sys.version_info >= (3, 8), reason='3.8 deals with CancelledError differently')
async def test_cancel_error(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        if ctx['job_try'] == 1:
            raise asyncio.CancelledError()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 1

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ↻ testing:retry cancelled, will be run again' in log


async def test_retry_job_error(arq_redis: ArqRedis, worker, caplog):
    async def retry(ctx):
        if ctx['job_try'] == 1:
            raise RetryJob()

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('retry', _job_id='testing')
    worker: Worker = worker(functions=[func(retry, name='retry')])
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 1

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ↻ testing:retry cancelled, will be run again' in log


async def test_job_expired(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    await arq_redis.delete(job_key_prefix + 'testing')
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'job testing expired' in log


async def test_job_expired_run_check(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    await arq_redis.delete(job_key_prefix + 'testing')
    worker: Worker = worker(functions=[foobar])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()

    assert str(exc_info.value) in {
        "1 job failed JobExecutionFailed('job expired',)",  # python 3.6
        "1 job failed JobExecutionFailed('job expired')",  # python 3.7
    }
    assert exc_info.value.count == 1
    assert len(exc_info.value.job_results) == 1
    assert exc_info.value.job_results[0].result == JobExecutionFailed('job expired')


@pytest.mark.parametrize(
    'extra_job_expiry,wait_time',
    [
        (None, expires_extra_ms),
        (1_000_000, 1_000_000),
        (10_000_000, 10_000_000),
        (999_999_999, 999_999_999),
    ],
)
async def test_default_job_expiry(arq_redis: ArqRedis, worker, caplog, extra_job_expiry, wait_time):
    """Test that jobs have a default expiry time based on the expires_extra_ms property in
    ArqRedis."""
    caplog.set_level(logging.INFO)
    if extra_job_expiry is not None:
        arq_redis.expires_extra_ms = extra_job_expiry
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    time_to_live_ms = await arq_redis.pttl(job_key_prefix + 'testing')
    assert time_to_live_ms == pytest.approx(wait_time)


async def test_job_old(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing', _defer_by=-2)
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert log.endswith('  0.XXs → testing:foobar() delayed=2.XXs\n' '  0.XXs ← testing:foobar ● 42')


async def test_retry_repr():
    assert str(Retry(123)) == '<Retry defer 123.00s>'


async def test_str_function(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('asyncio.sleep', _job_id='testing')
    worker: Worker = worker(functions=['asyncio.sleep'])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '0.XXs ! testing:asyncio.sleep failed, TypeError' in log


async def test_startup_shutdown(arq_redis: ArqRedis, worker):
    calls = []

    async def startup(ctx):
        calls.append('startup')

    async def shutdown(ctx):
        calls.append('shutdown')

    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar], on_startup=startup, on_shutdown=shutdown)
    await worker.main()
    await worker.close()

    assert calls == ['startup', 'shutdown']


class CustomError(RuntimeError):
    def extra(self):
        return {'x': 'y'}


async def error_function(ctx):
    raise CustomError('this is the error')


async def test_exc_extra(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('error_function', _job_id='testing')
    worker: Worker = worker(functions=[error_function])
    await worker.main()
    assert worker.jobs_failed == 1

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '0.XXs ! testing:error_function failed, CustomError: this is the error' in log
    error = next(r for r in caplog.records if r.levelno == logging.ERROR)
    assert error.extra == {'x': 'y'}


async def test_unpickleable(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)

    class Foo:
        pass

    async def example(ctx):
        return Foo()

    await arq_redis.enqueue_job('example', _job_id='testing')
    worker: Worker = worker(functions=[func(example, name='example')])
    await worker.main()

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'error serializing result of testing:example' in log


async def test_log_health_check(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    worker: Worker = worker(functions=[foobar], health_check_interval=0)
    await worker.main()
    await worker.main()
    await worker.main()
    assert worker.jobs_complete == 1

    assert 'j_complete=1 j_failed=0 j_retried=0 j_ongoing=0 queued=0' in caplog.text
    # assert log.count('recording health') == 1 can happen more than once due to redis pool size
    assert 'recording health' in caplog.text


async def test_remain_keys(arq_redis: ArqRedis, worker, create_pool):
    redis2 = await create_pool(RedisSettings())
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    assert sorted(await redis2.keys('*')) == [b'arq:job:testing', b'arq:queue']
    worker: Worker = worker(functions=[foobar])
    await worker.main()
    assert sorted(await redis2.keys('*')) == [b'arq:queue:health-check', b'arq:result:testing']
    await worker.close()
    assert sorted(await redis2.keys('*')) == [b'arq:result:testing']


async def test_remain_keys_no_results(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    assert sorted(await arq_redis.keys('*')) == [b'arq:job:testing', b'arq:queue']
    worker: Worker = worker(functions=[func(foobar, keep_result=0)])
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == [b'arq:queue:health-check']


async def test_remain_keys_keep_results_forever_in_function(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    assert sorted(await arq_redis.keys('*')) == [b'arq:job:testing', b'arq:queue']
    worker: Worker = worker(functions=[func(foobar, keep_result_forever=True)])
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == [b'arq:queue:health-check', b'arq:result:testing']
    ttl_result = await arq_redis.ttl('arq:result:testing')
    assert ttl_result == -1


async def test_remain_keys_keep_results_forever(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='testing')
    assert sorted(await arq_redis.keys('*')) == [b'arq:job:testing', b'arq:queue']
    worker: Worker = worker(functions=[func(foobar)], keep_result_forever=True)
    await worker.main()
    assert sorted(await arq_redis.keys('*')) == [b'arq:queue:health-check', b'arq:result:testing']
    ttl_result = await arq_redis.ttl('arq:result:testing')
    assert ttl_result == -1


async def test_run_check_passes(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar')
    await arq_redis.enqueue_job('foobar')
    worker: Worker = worker(functions=[func(foobar, name='foobar')])
    assert 2 == await worker.run_check()


async def test_run_check_error(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    with pytest.raises(FailedJobs, match=r"1 job failed TypeError\('my type error'"):
        await worker.run_check()


async def test_run_check_error2(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('fails')
    await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    with pytest.raises(FailedJobs, match='2 jobs failed:\n') as exc_info:
        await worker.run_check()
    assert len(exc_info.value.job_results) == 2


async def test_keep_result_ms(arq_redis: ArqRedis, worker):
    async def return_something(ctx):
        return 1

    await arq_redis.enqueue_job('return_something')
    worker: Worker = worker(functions=[func(return_something, name='return_something')], keep_result=3600.15)
    await worker.main()
    assert (worker.jobs_complete, worker.jobs_failed, worker.jobs_retried) == (1, 0, 0)


async def test_return_exception(arq_redis: ArqRedis, worker):
    async def return_error(ctx):
        return TypeError('xxx')

    j = await arq_redis.enqueue_job('return_error')
    worker: Worker = worker(functions=[func(return_error, name='return_error')])
    await worker.main()
    assert (worker.jobs_complete, worker.jobs_failed, worker.jobs_retried) == (1, 0, 0)
    r = await j.result(poll_delay=0)
    assert isinstance(r, TypeError)
    info = await j.result_info()
    assert info.success is True


async def test_error_success(arq_redis: ArqRedis, worker):
    j = await arq_redis.enqueue_job('fails')
    worker: Worker = worker(functions=[func(fails, name='fails')])
    await worker.main()
    assert (worker.jobs_complete, worker.jobs_failed, worker.jobs_retried) == (0, 1, 0)
    info = await j.result_info()
    assert info.success is False


async def test_many_jobs_expire(arq_redis: ArqRedis, worker, caplog):
    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foobar')
    await asyncio.gather(*[arq_redis.zadd(default_queue_name, {f'testing-{i}': 1}) for i in range(100)])
    worker: Worker = worker(functions=[foobar])
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 100
    assert worker.jobs_retried == 0

    log = '\n'.join(r.message for r in caplog.records)
    assert 'job testing-0 expired' in log
    assert log.count(' expired') == 100


async def test_repeat_job_result(arq_redis: ArqRedis, worker):
    j1 = await arq_redis.enqueue_job('foobar', _job_id='job_id')
    assert isinstance(j1, Job)
    assert await j1.status() == JobStatus.queued

    assert await arq_redis.enqueue_job('foobar', _job_id='job_id') is None

    await worker(functions=[foobar]).run_check()
    assert await j1.status() == JobStatus.complete

    assert await arq_redis.enqueue_job('foobar', _job_id='job_id') is None


async def test_queue_read_limit_equals_max_jobs(arq_redis: ArqRedis, worker):
    for _ in range(4):
        await arq_redis.enqueue_job('foobar')

    assert await arq_redis.zcard(default_queue_name) == 4
    worker: Worker = worker(functions=[foobar], queue_read_limit=2)
    assert worker.queue_read_limit == 2
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 2
    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 0
    assert worker.jobs_complete == 4
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_queue_read_limit_calc(worker):
    assert worker(functions=[foobar], queue_read_limit=2, max_jobs=1).queue_read_limit == 2
    assert worker(functions=[foobar], queue_read_limit=200, max_jobs=1).queue_read_limit == 200
    assert worker(functions=[foobar], max_jobs=18).queue_read_limit == 100
    assert worker(functions=[foobar], max_jobs=22).queue_read_limit == 110


async def test_custom_queue_read_limit(arq_redis: ArqRedis, worker):
    for _ in range(4):
        await arq_redis.enqueue_job('foobar')

    assert await arq_redis.zcard(default_queue_name) == 4
    worker: Worker = worker(functions=[foobar], max_jobs=4, queue_read_limit=2)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 2
    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    await worker._poll_iteration()
    await asyncio.sleep(0.1)
    assert await arq_redis.zcard(default_queue_name) == 0
    assert worker.jobs_complete == 4
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_custom_serializers(arq_redis_msgpack: ArqRedis, worker):
    j = await arq_redis_msgpack.enqueue_job('foobar', _job_id='job_id')
    worker: Worker = worker(
        functions=[foobar], job_serializer=msgpack.packb, job_deserializer=functools.partial(msgpack.unpackb, raw=False)
    )
    info = await j.info()
    assert info.function == 'foobar'
    assert await worker.run_check() == 1
    assert await j.result() == 42
    r = await j.info()
    assert r.result == 42


class UnpickleFails:
    def __init__(self, v):
        self.v = v

    def __setstate__(self, state):
        raise ValueError('this broke')


@pytest.mark.skipif(sys.version_info < (3, 7), reason='repr(exc) is ugly in 3.6')
async def test_deserialization_error(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', UnpickleFails('hello'), _job_id='job_id')
    worker: Worker = worker(functions=[foobar])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check()
    assert str(exc_info.value) == "1 job failed DeserializationError('unable to deserialize job')"


async def test_incompatible_serializers_1(arq_redis_msgpack: ArqRedis, worker):
    await arq_redis_msgpack.enqueue_job('foobar', _job_id='job_id')
    worker: Worker = worker(functions=[foobar])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0


async def test_incompatible_serializers_2(arq_redis: ArqRedis, worker):
    await arq_redis.enqueue_job('foobar', _job_id='job_id')
    worker: Worker = worker(
        functions=[foobar], job_serializer=msgpack.packb, job_deserializer=functools.partial(msgpack.unpackb, raw=False)
    )
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0


async def test_max_jobs_completes(arq_redis: ArqRedis, worker):
    v = 0

    async def raise_second_time(ctx):
        nonlocal v
        v += 1
        if v > 1:
            raise ValueError('xxx')

    await arq_redis.enqueue_job('raise_second_time')
    await arq_redis.enqueue_job('raise_second_time')
    await arq_redis.enqueue_job('raise_second_time')
    worker: Worker = worker(functions=[func(raise_second_time, name='raise_second_time')])
    with pytest.raises(FailedJobs) as exc_info:
        await worker.run_check(max_burst_jobs=3)
    assert repr(exc_info.value).startswith('<2 jobs failed:')


async def test_max_bursts_sub_call(arq_redis: ArqRedis, worker, caplog):
    async def foo(ctx, v):
        return v + 1

    async def bar(ctx, v):
        await ctx['redis'].enqueue_job('foo', v + 1)

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('bar', 10)
    worker: Worker = worker(functions=[func(foo, name='foo'), func(bar, name='bar')])
    assert await worker.run_check(max_burst_jobs=1) == 1
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert 'bar(10)' in caplog.text
    assert 'foo' in caplog.text


async def test_max_bursts_multiple(arq_redis: ArqRedis, worker, caplog):
    async def foo(ctx, v):
        return v + 1

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foo', 1)
    await arq_redis.enqueue_job('foo', 2)
    worker: Worker = worker(functions=[func(foo, name='foo')])
    assert await worker.run_check(max_burst_jobs=1) == 1
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert 'foo(1)' in caplog.text
    assert 'foo(2)' not in caplog.text


async def test_max_bursts_dont_get(arq_redis: ArqRedis, worker):
    async def foo(ctx, v):
        return v + 1

    await arq_redis.enqueue_job('foo', 1)
    await arq_redis.enqueue_job('foo', 2)
    worker: Worker = worker(functions=[func(foo, name='foo')])

    worker.max_burst_jobs = 0
    assert len(worker.tasks) == 0
    await worker._poll_iteration()
    assert len(worker.tasks) == 0


async def test_non_burst(arq_redis: ArqRedis, worker, caplog, loop):
    async def foo(ctx, v):
        return v + 1

    caplog.set_level(logging.INFO)
    await arq_redis.enqueue_job('foo', 1, _job_id='testing')
    worker: Worker = worker(functions=[func(foo, name='foo')])
    worker.burst = False
    t = loop.create_task(worker.main())
    await asyncio.sleep(0.1)
    t.cancel()
    assert worker.jobs_complete == 1
    assert worker.jobs_retried == 0
    assert worker.jobs_failed == 0
    assert '← testing:foo ● 2' in caplog.text


async def test_multi_exec(arq_redis: ArqRedis, worker, caplog):
    c = 0

    async def foo(ctx, v):
        nonlocal c
        c += 1
        return v + 1

    caplog.set_level(logging.DEBUG, logger='arq.worker')
    await arq_redis.enqueue_job('foo', 1, _job_id='testing')
    worker: Worker = worker(functions=[func(foo, name='foo')])
    await asyncio.gather(*[worker.start_jobs([b'testing']) for _ in range(5)])
    # debug(caplog.text)
    await worker.main()
    assert c == 1
    # assert 'multi-exec error, job testing already started elsewhere' in caplog.text
    # assert 'WatchVariableError' not in caplog.text


async def test_abort_job(arq_redis: ArqRedis, worker, caplog, loop):
    async def longfunc(ctx):
        await asyncio.sleep(3600)

    async def wait_and_abort(job, delay=0.1):
        await asyncio.sleep(delay)
        assert await job.abort() is True

    caplog.set_level(logging.INFO)
    await arq_redis.zadd(abort_jobs_ss, {b'foobar': int(1e9)})
    job = await arq_redis.enqueue_job('longfunc', _job_id='testing')

    worker: Worker = worker(functions=[func(longfunc, name='longfunc')], allow_abort_jobs=True, poll_delay=0.1)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await asyncio.gather(wait_and_abort(job), worker.main())
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0
    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs → testing:longfunc()\n  X.XXs ⊘ testing:longfunc aborted' in log
    assert worker.aborting_tasks == set()
    assert worker.tasks == {}
    assert worker.job_tasks == {}


async def test_abort_job_which_is_not_in_queue(arq_redis: ArqRedis):
    job = Job(job_id='testing', redis=arq_redis)
    assert await job.abort() is False


async def test_abort_job_before(arq_redis: ArqRedis, worker, caplog, loop):
    async def longfunc(ctx):
        await asyncio.sleep(3600)

    caplog.set_level(logging.INFO)

    job = await arq_redis.enqueue_job('longfunc', _job_id='testing')

    worker: Worker = worker(functions=[func(longfunc, name='longfunc')], allow_abort_jobs=True, poll_delay=0.1)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    with pytest.raises(asyncio.TimeoutError):
        await job.abort(timeout=0)
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0
    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ⊘ testing:longfunc aborted before start' in log
    await worker.main()
    assert worker.aborting_tasks == set()
    assert worker.job_tasks == {}
    assert worker.tasks == {}


async def test_abort_deferred_job_before(arq_redis: ArqRedis, worker, caplog, loop):
    async def longfunc(ctx):
        await asyncio.sleep(3600)

    caplog.set_level(logging.INFO)

    job = await arq_redis.enqueue_job('longfunc', _job_id='testing', _defer_until=datetime.utcnow() + timedelta(days=1))

    worker: Worker = worker(functions=[func(longfunc, name='longfunc')], allow_abort_jobs=True, poll_delay=0.1)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    with pytest.raises(asyncio.TimeoutError):
        await job.abort(timeout=0)
    await worker.main()

    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0
    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ⊘ testing:longfunc aborted before start' in log
    await worker.main()
    assert worker.aborting_tasks == set()
    assert worker.job_tasks == {}
    assert worker.tasks == {}


async def test_not_abort_job(arq_redis: ArqRedis, worker, caplog, loop):
    async def shortfunc(ctx):
        await asyncio.sleep(0.2)

    async def wait_and_abort(job, delay=0.1):
        await asyncio.sleep(delay)
        assert await job.abort() is False

    caplog.set_level(logging.INFO)
    job = await arq_redis.enqueue_job('shortfunc', _job_id='testing')

    worker: Worker = worker(functions=[func(shortfunc, name='shortfunc')], poll_delay=0.1)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await asyncio.gather(wait_and_abort(job), worker.main())
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs → testing:shortfunc()\n  X.XXs ← testing:shortfunc ●' in log
    await worker.main()
    assert worker.aborting_tasks == set()
    assert worker.tasks == {}
    assert worker.job_tasks == {}


async def test_job_timeout(arq_redis: ArqRedis, worker, caplog):
    async def longfunc(ctx):
        await asyncio.sleep(0.3)

    caplog.set_level(logging.ERROR)
    await arq_redis.enqueue_job('longfunc', _job_id='testing')
    worker: Worker = worker(functions=[func(longfunc, name='longfunc')], job_timeout=0.2, poll_delay=0.1)
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0
    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs ! testing:longfunc failed, TimeoutError:' in log


async def test_on_job(arq_redis: ArqRedis, worker):
    result = {'called': 0}

    async def on_start(ctx):
        assert ctx['job_id'] == 'testing'
        result['called'] += 1

    async def on_end(ctx):
        assert ctx['job_id'] == 'testing'
        result['called'] += 1

    async def after_end(ctx):
        assert ctx['job_id'] == 'testing'
        result['called'] += 2

    async def test(ctx):
        return

    await arq_redis.enqueue_job('func', _job_id='testing')
    worker: Worker = worker(
        functions=[func(test, name='func')],
        on_job_start=on_start,
        on_job_end=on_end,
        after_job_end=after_end,
        job_timeout=0.2,
        poll_delay=0.1,
    )
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    assert result['called'] == 0
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    assert result['called'] == 4


async def test_job_cancel_on_max_jobs(arq_redis: ArqRedis, worker, caplog):
    async def longfunc(ctx):
        await asyncio.sleep(3600)

    async def wait_and_abort(job, delay=0.1):
        await asyncio.sleep(delay)
        assert await job.abort() is True

    caplog.set_level(logging.INFO)
    await arq_redis.zadd(abort_jobs_ss, {b'foobar': int(1e9)})
    job = await arq_redis.enqueue_job('longfunc', _job_id='testing')

    worker: Worker = worker(
        functions=[func(longfunc, name='longfunc')], allow_abort_jobs=True, poll_delay=0.1, max_jobs=1
    )
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
    await asyncio.gather(wait_and_abort(job), worker.main())
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 1
    assert worker.jobs_retried == 0
    log = re.sub(r'\d+.\d\ds', 'X.XXs', '\n'.join(r.message for r in caplog.records))
    assert 'X.XXs → testing:longfunc()\n  X.XXs ⊘ testing:longfunc aborted' in log
    assert worker.aborting_tasks == set()
    assert worker.tasks == {}
    assert worker.job_tasks == {}


async def test_worker_timezone_defaults_to_system_timezone(worker):
    worker = worker(functions=[func(foobar)])
    assert worker.timezone is not None
    assert worker.timezone == datetime.now().astimezone().tzinfo


@pytest.mark.parametrize(
    'exception_thrown',
    [
        redis.exceptions.ConnectionError('Error while reading from host'),
        redis.exceptions.TimeoutError('Timeout reading from host'),
    ],
)
async def test_worker_retry(mocker, worker_retry, exception_thrown):
    # Testing redis exceptions, with retry settings specified
    worker = worker_retry(functions=[func(foobar)])

    # patch db read_response to mimic connection exceptions
    p = patch.object(worker.pool.connection_pool.connection_class, 'read_response', side_effect=exception_thrown)

    # baseline
    await worker.main()
    await worker._poll_iteration()

    # spy method handling call_with_retry failure
    spy = mocker.spy(worker.pool, '_disconnect_raise')

    try:
        # start patch
        p.start()

        # assert exception thrown
        with pytest.raises(type(exception_thrown)):
            await worker._poll_iteration()

        # assert retry counts and no exception thrown during '_disconnect_raise'
        assert spy.call_count == 4  # retries setting + 1
        assert spy.spy_exception is None

    finally:
        # stop patch to allow worker cleanup
        p.stop()


@pytest.mark.parametrize(
    'exception_thrown',
    [
        redis.exceptions.ConnectionError('Error while reading from host'),
        redis.exceptions.TimeoutError('Timeout reading from host'),
    ],
)
async def test_worker_crash(mocker, worker, exception_thrown):
    # Testing redis exceptions, no retry settings specified
    worker = worker(functions=[func(foobar)])

    # patch db read_response to mimic connection exceptions
    p = patch.object(worker.pool.connection_pool.connection_class, 'read_response', side_effect=exception_thrown)

    # baseline
    await worker.main()
    await worker._poll_iteration()

    # spy method handling call_with_retry failure
    spy = mocker.spy(worker.pool, '_disconnect_raise')

    try:
        # start patch
        p.start()

        # assert exception thrown
        with pytest.raises(type(exception_thrown)):
            await worker._poll_iteration()

        # assert no retry counts and exception thrown during '_disconnect_raise'
        assert spy.call_count == 1
        assert spy.spy_exception == exception_thrown

    finally:
        # stop patch to allow worker cleanup
        p.stop()
