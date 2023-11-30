import asyncio
import logging
import re
from datetime import datetime, timedelta, timezone
from random import random

import pytest
from pytest_mock import MockerFixture

import arq
from arq import Worker
from arq.constants import in_progress_key_prefix
from arq.cron import cron, next_cron

tz = timezone(offset=timedelta(hours=3))


@pytest.mark.parametrize(
    'previous,expected,kwargs',
    [
        (
            datetime(2016, 6, 1, 12, 10, 10, tzinfo=tz),
            datetime(2016, 6, 1, 12, 10, 20, microsecond=123_456, tzinfo=tz),
            dict(second=20),
        ),
        (
            datetime(2016, 6, 1, 12, 10, 10, tzinfo=tz),
            datetime(2016, 6, 1, 12, 11, 0, microsecond=123_456, tzinfo=tz),
            dict(minute=11),
        ),
        (
            datetime(2016, 6, 1, 12, 10, 10, tzinfo=tz),
            datetime(2016, 6, 1, 12, 10, 20, tzinfo=tz),
            dict(second=20, microsecond=0),
        ),
        (
            datetime(2016, 6, 1, 12, 10, 10, tzinfo=tz),
            datetime(2016, 6, 1, 12, 11, 0, tzinfo=tz),
            dict(minute=11, microsecond=0),
        ),
        (
            datetime(2016, 6, 1, 12, 10, 11, tzinfo=tz),
            datetime(2017, 6, 1, 12, 10, 10, microsecond=123_456, tzinfo=tz),
            dict(month=6, day=1, hour=12, minute=10, second=10),
        ),
        (
            datetime(2016, 6, 1, 12, 10, 10, microsecond=1, tzinfo=tz),
            datetime(2016, 7, 1, 12, 10, 10, tzinfo=tz),
            dict(day=1, hour=12, minute=10, second=10, microsecond=0),
        ),
        (
            datetime(2032, 1, 31, 0, 0, 0, tzinfo=tz),
            datetime(2032, 2, 28, 0, 0, 0, microsecond=123_456, tzinfo=tz),
            dict(day=28),
        ),
        (
            datetime(2032, 1, 1, 0, 5, tzinfo=tz),
            datetime(2032, 1, 1, 4, 0, microsecond=123_456, tzinfo=tz),
            dict(hour=4),
        ),
        (
            datetime(2032, 1, 1, 0, 0, tzinfo=tz),
            datetime(2032, 1, 1, 4, 2, microsecond=123_456, tzinfo=tz),
            dict(hour=4, minute={2, 4, 6}),
        ),
        (
            datetime(2032, 1, 1, 0, 5, tzinfo=tz),
            datetime(2032, 1, 1, 4, 2, microsecond=123_456, tzinfo=tz),
            dict(hour=4, minute={2, 4, 6}),
        ),
        (
            datetime(2032, 2, 5, 0, 0, 0, tzinfo=tz),
            datetime(2032, 3, 31, 0, 0, 0, microsecond=123_456, tzinfo=tz),
            dict(day=31),
        ),
        (
            datetime(2001, 1, 1, 0, 0, 0, tzinfo=tz),  # Monday
            datetime(2001, 1, 7, 0, 0, 0, microsecond=123_456, tzinfo=tz),
            dict(weekday='Sun'),  # Sunday
        ),
        (
            datetime(2001, 1, 1, 0, 0, 0, tzinfo=tz),
            datetime(2001, 1, 7, 0, 0, 0, microsecond=123_456, tzinfo=tz),
            dict(weekday=6),
        ),  # Sunday
        (
            datetime(2001, 1, 1, 0, 0, 0, tzinfo=tz),
            datetime(2001, 11, 7, 0, 0, 0, microsecond=123_456, tzinfo=tz),
            dict(month=11, weekday=2),
        ),
        (
            datetime(2001, 1, 1, 0, 0, 0, tzinfo=tz),
            datetime(2001, 1, 3, 0, 0, 0, microsecond=123_456, tzinfo=tz),
            dict(weekday='wed'),
        ),
    ],
)
def test_next_cron(previous, expected, kwargs):
    start = datetime.now()
    assert next_cron(previous, **kwargs) == expected
    diff = datetime.now() - start
    print(f'{diff.total_seconds() * 1000:0.3f}ms')


def test_next_cron_preserves_tzinfo():
    previous = datetime.fromisoformat('2016-06-01T12:10:10+02:00')
    assert previous.tzinfo is not None
    assert next_cron(previous, second=20).tzinfo is previous.tzinfo


def test_next_cron_invalid():
    with pytest.raises(ValueError):
        next_cron(datetime(2001, 1, 1, 0, 0, 0), weekday='monday')


@pytest.mark.parametrize(
    'max_previous,kwargs,expected',
    [
        (1, dict(microsecond=59867), datetime(2001, 1, 1, 0, 0, microsecond=59867)),
        (59, dict(second=28, microsecond=0), datetime(2023, 1, 1, 1, 59, 28)),
        (3600, dict(minute=10, second=20), datetime(2016, 6, 1, 12, 10, 20, microsecond=123_456)),
        (68400, dict(hour=3), datetime(2032, 1, 1, 3, 0, 0, microsecond=123_456)),
        (68400 * 60, dict(day=31, minute=59), datetime(2032, 3, 31, 0, 59, 0, microsecond=123_456)),
        (68400 * 7, dict(weekday='tues', minute=59), datetime(2032, 3, 30, 0, 59, 0, microsecond=123_456)),
        (
            68400 * 175,  # previous friday the 13th is February
            dict(day=13, weekday='fri', microsecond=1),
            datetime(2032, 8, 13, 0, 0, 0, microsecond=1),
        ),
        (68400 * 365, dict(month=10, day=4, hour=23), datetime(2032, 10, 4, 23, 0, 0, microsecond=123_456)),
        (
            1,
            dict(month=1, day=1, hour=0, minute=0, second=0, microsecond=69875),
            datetime(2001, 1, 1, 0, 0, microsecond=69875),
        ),
    ],
)
def test_next_cron_random(max_previous, kwargs, expected):
    for i in range(100):
        previous = expected - timedelta(seconds=0.9 + random() * max_previous)
        v = next_cron(previous, **kwargs)
        diff = v - previous
        if diff > timedelta(seconds=1):
            print(f'previous: {previous}, expected: {expected}, diff: {v - previous}')
            assert v == expected


async def foobar(ctx):
    return 42


@pytest.mark.parametrize('poll_delay', [0.0, 0.5, 0.9])
async def test_job_successful(worker, caplog, arq_redis, poll_delay):
    caplog.set_level(logging.INFO)
    worker: Worker = worker(cron_jobs=[cron(foobar, hour=1, run_at_startup=True)], poll_delay=poll_delay)
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '  0.XXs → cron:foobar()\n  0.XXs ← cron:foobar ● 42' in log

    # Assert the in-progress key still exists.
    keys = await arq_redis.keys(in_progress_key_prefix + '*')
    assert len(keys) == 1
    assert await arq_redis.pttl(keys[0]) > 0.0


async def test_calculate_next_is_called_with_aware_datetime(worker, mocker: MockerFixture):
    worker: Worker = worker(cron_jobs=[cron(foobar, hour=1)])
    spy_run_cron = mocker.spy(worker, worker.run_cron.__name__)

    await worker.main()

    assert spy_run_cron.call_args[0][0].tzinfo is not None
    assert worker.cron_jobs[0].next_run.tzinfo is not None


async def test_job_successful_on_specific_queue(worker, caplog):
    caplog.set_level(logging.INFO)
    worker: Worker = worker(
        queue_name='arq:test-cron-queue', cron_jobs=[cron(foobar, hour=1, run_at_startup=True)], poll_delay=0.5
    )
    await worker.main()
    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = re.sub(r'(\d+).\d\ds', r'\1.XXs', '\n'.join(r.message for r in caplog.records))
    assert '  0.XXs → cron:foobar()\n  0.XXs ← cron:foobar ● 42' in log


async def test_not_run(worker, caplog):
    caplog.set_level(logging.INFO)
    worker: Worker = worker(cron_jobs=[cron(foobar, hour=1, run_at_startup=False)])
    await worker.main()
    assert worker.jobs_complete == 0
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0

    log = '\n'.join(r.message for r in caplog.records)
    assert 'cron:foobar()' not in log


async def test_repr():
    cj = cron(foobar, hour=1, run_at_startup=True)
    assert str(cj).startswith('<CronJob name=cron:foobar coroutine=<function foobar at')


async def test_str_function():
    cj = cron('asyncio.sleep', hour=1, run_at_startup=True)
    assert str(cj).startswith('<CronJob name=cron:asyncio.sleep coroutine=<function sleep at')


async def test_cron_cancelled(worker, mocker):
    mocker.patch.object(arq.worker, 'keep_cronjob_progress', 0.1)

    async def try_sleep(ctx):
        if ctx['job_try'] == 1:
            raise asyncio.CancelledError

    worker: Worker = worker(
        cron_jobs=[cron(try_sleep, microsecond=20, run_at_startup=True, max_tries=2)],
        poll_delay=0.01,
    )
    await worker.main()
    assert worker.jobs_complete in (1, 2)
    assert worker.jobs_retried == worker.jobs_complete
    assert worker.jobs_failed == 0


async def barfoo(ctx):
    """In order to test cron job singleton, we must have two different functions when bursting"""
    return 24


async def test_job_custom_id(worker):
    """
    Test that two different functions with the same job_id, will only be executed once.
    """
    worker: Worker = worker(
        cron_jobs=[
            cron(barfoo, minute=10, run_at_startup=True, job_id='singleton_job'),
            cron(foobar, minute=20, run_at_startup=True, job_id='singleton_job'),
        ],
        poll_delay=0.01,
    )
    await worker.main()

    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_job_same_function_different_id(worker):
    """
    Set a different ID on the same function job, which runs at startup and ensure both functions are run.
    See next test for behaviour without setting job_id.
    """
    worker: Worker = worker(
        cron_jobs=[
            cron(foobar, minute=10, run_at_startup=True, job_id='custom_id'),
            cron(foobar, minute=20, run_at_startup=True, job_id='custom_id2'),
        ],
        poll_delay=0.01,
    )
    await worker.main()

    assert worker.jobs_complete == 2
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0


async def test_run_at_startup_no_id_only_runs_once(worker):
    """
    Without a custom job ID, and `run_at_startup=True` on two jobs, it will only execute once, since the job_id
    will be equal, and should only run once.
    """
    worker: Worker = worker(
        cron_jobs=[cron(foobar, minute=10, run_at_startup=True), cron(foobar, minute=20, run_at_startup=True)],
        poll_delay=0.01,
    )
    await worker.main()

    assert worker.jobs_complete == 1
    assert worker.jobs_failed == 0
    assert worker.jobs_retried == 0
