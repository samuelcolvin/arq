import logging
import re
import sys
from dataclasses import asdict
from datetime import timedelta

import pytest
from pydantic import BaseModel, field_validator
from redis.asyncio import ConnectionError, ResponseError

import arq.typing
import arq.utils
from arq.connections import RedisSettings, log_redis_info

from .conftest import SetEnv


def test_settings_changed():
    settings = RedisSettings(port=123)
    assert settings.port == 123
    assert (
        "RedisSettings(host='localhost', port=123, unix_socket_path=None, database=0, username=None, password=None, "
        "ssl=False, ssl_keyfile=None, ssl_certfile=None, ssl_cert_reqs='required', ssl_ca_certs=None, "
        'ssl_ca_data=None, ssl_check_hostname=False, conn_timeout=1, conn_retries=5, conn_retry_delay=1, '
        "max_connections=None, sentinel=False, sentinel_master='mymaster', "
        'retry_on_timeout=False, retry_on_error=None, retry=None)'
    ) == str(settings)


async def test_redis_timeout(mocker, create_pool):
    mocker.spy(arq.utils.asyncio, 'sleep')
    with pytest.raises(ConnectionError):
        await create_pool(RedisSettings(port=0, conn_retry_delay=0))
    assert arq.utils.asyncio.sleep.call_count == 5


async def test_redis_timeout_and_retry_many_times(mocker, create_pool):
    mocker.spy(arq.utils.asyncio, 'sleep')
    default_recursion_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(100)
    try:
        with pytest.raises(ConnectionError):
            await create_pool(RedisSettings(port=0, conn_retry_delay=0, conn_retries=150))
        assert arq.utils.asyncio.sleep.call_count == 150
    finally:
        sys.setrecursionlimit(default_recursion_limit)


@pytest.mark.skip(reason='this breaks many other tests as low level connections remain after failed connection')
async def test_redis_sentinel_failure(create_pool, cancel_remaining_task, mocker):
    settings = RedisSettings()
    settings.host = [('localhost', 6379), ('localhost', 6379)]
    settings.sentinel = True
    with pytest.raises(ResponseError, match='unknown command `SENTINEL`'):
        await create_pool(settings)


async def test_redis_success_log(caplog, create_pool):
    caplog.set_level(logging.INFO)
    settings = RedisSettings()
    pool = await create_pool(settings)
    assert 'redis connection successful' not in [r.message for r in caplog.records]
    await pool.close(close_connection_pool=True)

    pool = await create_pool(settings, retry=1)
    assert 'redis connection successful' in [r.message for r in caplog.records]
    await pool.close(close_connection_pool=True)


async def test_redis_log(create_pool):
    redis = await create_pool(RedisSettings())
    await redis.flushall()
    await redis.set(b'a', b'1')
    await redis.set(b'b', b'2')

    log_msgs = []

    def _log(s):
        log_msgs.append(s)

    await log_redis_info(redis, _log)
    assert len(log_msgs) == 1
    assert re.search(r'redis_version=\d\.', log_msgs[0]), log_msgs
    assert log_msgs[0].endswith(' db_keys=2')


def test_truncate():
    assert arq.utils.truncate('123456', 4) == '123…'


def test_args_to_string():
    assert arq.utils.args_to_string((), {'d': 4}) == 'd=4'
    assert arq.utils.args_to_string((1, 2, 3), {}) == '1, 2, 3'
    assert arq.utils.args_to_string((1, 2, 3), {'d': 4}) == '1, 2, 3, d=4'


@pytest.mark.parametrize(
    'input,output', [(timedelta(days=1), 86_400_000), (42, 42000), (42.123, 42123), (42.123_987, 42124), (None, None)]
)
def test_to_ms(input, output):
    assert arq.utils.to_ms(input) == output


@pytest.mark.parametrize('input,output', [(timedelta(days=1), 86400), (42, 42), (42.123, 42.123), (None, None)])
def test_to_seconds(input, output):
    assert arq.utils.to_seconds(input) == output


def test_typing():
    assert 'OptionType' in arq.typing.__all__


def test_redis_settings_validation():
    class Settings(BaseModel, arbitrary_types_allowed=True):
        redis_settings: RedisSettings

        @field_validator('redis_settings', mode='before')
        def parse_redis_settings(cls, v):
            if isinstance(v, str):
                return RedisSettings.from_dsn(v)
            else:
                return v

    s1 = Settings(redis_settings='redis://foobar:123/4')
    assert s1.redis_settings.host == 'foobar'
    assert s1.redis_settings.port == 123
    assert s1.redis_settings.database == 4
    assert s1.redis_settings.ssl is False

    s2 = Settings(redis_settings={'host': 'testing.com'})
    assert s2.redis_settings.host == 'testing.com'
    assert s2.redis_settings.port == 6379

    with pytest.raises(ValueError, match='1 validation error for Settings\nredis_settings.ssl'):
        Settings(redis_settings={'ssl': 123})

    s3 = Settings(redis_settings={'ssl': True})
    assert s3.redis_settings.host == 'localhost'
    assert s3.redis_settings.ssl is True

    s4 = Settings(redis_settings='redis://user:pass@foobar')
    assert s4.redis_settings.host == 'foobar'
    assert s4.redis_settings.username == 'user'
    assert s4.redis_settings.password == 'pass'

    s5 = Settings(redis_settings={'unix_socket_path': '/tmp/redis.sock'})
    assert s5.redis_settings.unix_socket_path == '/tmp/redis.sock'
    assert s5.redis_settings.database == 0

    s6 = Settings(redis_settings='unix:///tmp/redis.socket?db=6')
    assert s6.redis_settings.unix_socket_path == '/tmp/redis.socket'
    assert s6.redis_settings.database == 6


def test_ms_to_datetime_tz(env: SetEnv):
    arq.utils.get_tz.cache_clear()
    env.set('ARQ_TIMEZONE', 'Asia/Shanghai')
    env.set('TIMEZONE', 'Europe/Berlin')  # lower priority as per `timezone_keys`
    dt = arq.utils.ms_to_datetime(1_647_345_420_000)  # 11.57 UTC
    assert dt.isoformat() == '2022-03-15T19:57:00+08:00'
    assert dt.tzinfo.zone == 'Asia/Shanghai'

    # should have no effect due to caching
    env.set('ARQ_TIMEZONE', 'Europe/Berlin')
    dt = arq.utils.ms_to_datetime(1_647_345_420_000)
    assert dt.isoformat() == '2022-03-15T19:57:00+08:00'


def test_ms_to_datetime_no_tz(env: SetEnv):
    arq.utils.get_tz.cache_clear()
    dt = arq.utils.ms_to_datetime(1_647_345_420_000)  # 11.57 UTC
    assert dt.isoformat() == '2022-03-15T11:57:00+00:00'

    # should have no effect due to caching
    env.set('ARQ_TIMEZONE', 'Europe/Berlin')
    dt = arq.utils.ms_to_datetime(1_647_345_420_000)
    assert dt.isoformat() == '2022-03-15T11:57:00+00:00'


def test_ms_to_datetime_tz_invalid(env: SetEnv, caplog):
    arq.utils.get_tz.cache_clear()
    env.set('ARQ_TIMEZONE', 'foobar')
    caplog.set_level(logging.WARNING)
    dt = arq.utils.ms_to_datetime(1_647_345_420_000)
    assert dt.isoformat() == '2022-03-15T11:57:00+00:00'
    assert "unknown timezone: 'foobar'\n" in caplog.text


def test_import_string_valid():
    sqrt = arq.utils.import_string('math.sqrt')
    assert sqrt(4) == 2


def test_import_string_invalid_short():
    with pytest.raises(ImportError, match='"foobar" doesn\'t look like a module path'):
        arq.utils.import_string('foobar')


def test_import_string_invalid_missing():
    with pytest.raises(ImportError, match='Module "math" does not define a "foobar" attribute'):
        arq.utils.import_string('math.foobar')


def test_settings_plain():
    settings = RedisSettings()
    assert asdict(settings) == {
        'host': 'localhost',
        'port': 6379,
        'unix_socket_path': None,
        'database': 0,
        'username': None,
        'password': None,
        'ssl': False,
        'ssl_keyfile': None,
        'ssl_certfile': None,
        'ssl_cert_reqs': 'required',
        'ssl_ca_certs': None,
        'ssl_ca_data': None,
        'ssl_check_hostname': False,
        'conn_timeout': 1,
        'conn_retries': 5,
        'conn_retry_delay': 1,
        'sentinel': False,
        'sentinel_master': 'mymaster',
        'retry_on_timeout': False,
        'retry_on_error': None,
        'retry': None,
        'max_connections': None,
    }


def test_settings_from_socket_dsn():
    settings = RedisSettings.from_dsn('unix:///run/redis/redis.sock')
    # insert_assert(asdict(settings))
    assert asdict(settings) == {
        'host': 'localhost',
        'port': 6379,
        'unix_socket_path': '/run/redis/redis.sock',
        'database': 0,
        'username': None,
        'password': None,
        'ssl': False,
        'ssl_keyfile': None,
        'ssl_certfile': None,
        'ssl_cert_reqs': 'required',
        'ssl_ca_certs': None,
        'ssl_ca_data': None,
        'ssl_check_hostname': False,
        'conn_timeout': 1,
        'conn_retries': 5,
        'conn_retry_delay': 1,
        'sentinel': False,
        'sentinel_master': 'mymaster',
        'retry_on_timeout': False,
        'retry_on_error': None,
        'retry': None,
        'max_connections': None,
    }


def test_lazy_str():
    called = False

    def get_str() -> str:
        nonlocal called
        called = True
        return 'get_str'

    lazy_str = arq.utils.LazyStr(get_str)
    assert not called
    assert str(lazy_str) == 'get_str'
    assert called
