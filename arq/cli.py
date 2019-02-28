import logging.config
from typing import Type

import click
from pydantic.utils import import_string

from .logs import default_log_config
from .version import VERSION
from .worker import BaseWorkerSettings, run_worker


burst_help = 'Batch mode: exit once no jobs are found in any queue.'
health_check_help = 'Health Check: run a health check and exit'
verbose_help = 'Enable verbose output.'


@click.command()
@click.version_option(VERSION, '-V', '--version', prog_name='arq')
@click.argument('worker-settings', type=str, required=True)
@click.option('--check', is_flag=True, help=health_check_help)
@click.option('-v', '--verbose', is_flag=True, help=verbose_help)
def cli(*, worker_settings, check, verbose):
    """
    Job queues in python with asyncio, redis and msgpack.

    CLI to run the arq worker.
    """
    worker_settings: Type[BaseWorkerSettings] = import_string(worker_settings)
    logging.config.dictConfig(default_log_config(verbose))

    # if check:
    #     exit(worker.check_health())
    # else:
    run_worker(worker_settings)
