arq
===

.. toctree::
   :maxdepth: 2

|pypi| |license|

Current Version: |version|

Job queues and RPC in python with asyncio and redis.

*arq* was conceived as a simple, modern and performant successor to rq_.

.. warning::

   In ``v0.16`` *arq* was **COMPLETELY REWRITTEN** to use an entirely different approach to registering workers,
   enqueueing jobs and processing jobs. You will need to either keep using ``v0.15`` or entirely rewrite your *arq*
   integration to use ``v0.16``.

   See `here <./old/index.html>`_ for old docs.

Why use *arq*?

**non-blocking**
    *arq* is built using python 3's asyncio_ allowing
    non-blocking job enqueuing and execution. Multiple jobs (potentially hundreds) can be run simultaneously
    using a pool of *asyncio* ``Tasks``.

**powerful-features**
    Deferred execution, easy retrying of jobs, and pessimistic execution (:ref:`see below <usage>`)
    means *arq* is great for critical jobs that **must** be completed.

**fast**
    Asyncio and no forking make *arq* around 7x faster than
    *rq* for short jobs with no io. With io that might increase to around 40x
    faster. (TODO)

**elegant**
    I'm a long time contributor to and user of `rq`_, *arq* is designed to be simpler, clearer and more powerful.

**small**
    and easy to reason with - currently *arq* is only about 700 lines, that won't change significantly.

Install
-------

Just::

    pip install arq

Redesigned to be less elegant?
------------------------------

The approach used in *arq* ``v0.16`` of enqueueing jobs by name rather than "just calling a function" and knowing it
will be called on the worker (as used in *arq* ``<= v0.15``, rq, celery et al.) might seem less elegant,
but it's for good reason.

This approach means your frontend (calling the worker) doesn't need access to the worker code,
meaning better code separation and possibly smaller images etc.

.. _usage:

Usage
-----

.. warning::

   **Jobs may be called more than once!**

   *arq* v0.16 has what I'm calling "pessimistic execution": jobs aren't removed from the queue until they've either
   succeeded or failed. If the worker shuts down, the job will be cancelled immediately and will remain in the queue
   to be run again when the worker starts up again (or run by another worker which is still running).

   (This differs from other similar libraries like *arq* ``<= v0.15``, rq, celery et al. where jobs generally don't get
   rerun when a worker shuts down. This in turn requires complex logic to try and let jobs finish before
   shutting down (I wrote the ``HerokuWorker`` for rq), however this never really works unless either: all jobs take
   less than 6 seconds or your worker never shuts down when a job is running (impossible).)

   All *arq* jobs should therefore be designed to cope with being called repeatedly if they're cancelled,
   eg. use database transactions, idempotency keys or redis to mark when an API request or similar has succeeded
   to avoid making it twice.

   **In summary:** sometimes *exactly once* can be hard or impossible, *arq* favours multiple times over zero times.

Simple Usage
............

.. literalinclude:: examples/main_demo.py

(This script is complete, it should run "as is" both to enqueue jobs and run them)

To enqueue the jobs, simply run the script::

    python demo.py

To execute the jobs, either after running ``demo.py`` or before/during::

    arq demo.WorkerSettings

Append ``--burst`` to stop the worker once all jobs have finished. See :class:`arq.worker.Worker` for more available
properties of ``WorkerSettings``.

You can also watch for changes and reload the worker when the source changes::

    arq demo.WorkerSettings --watch path/to/src

This requires watchfiles_ to be installed (``pip install watchfiles``).

For details on the *arq* CLI::

    arq --help

Startup & Shutdown coroutines
.............................

The ``on_startup`` and ``on_shutdown`` coroutines are provided as a convenient way to run logic as the worker
starts and finishes, see :class:`arq.worker.Worker`.

For example, in the above example ``session`` is created once when the work starts up and is then used in subsequent
jobs.

Deferring Jobs
..............

By default, when a job is enqueued it will run as soon as possible (provided a worker is running). However
you can schedule jobs to run in the future, either by a given duration (``_defer_by``) or
at a particular time ``_defer_until``, see :func:`arq.connections.ArqRedis.enqueue_job`.

.. literalinclude:: examples/deferred.py

Job Uniqueness
..............

Sometimes you want a job to only be run once at a time (eg. a backup) or once for a given parameter (eg. generating
invoices for a particular company).

*arq* supports this via custom job ids, see :func:`arq.connections.ArqRedis.enqueue_job`. It guarantees
that a job with a particular ID cannot be enqueued again until its execution has finished and its result has cleared. To control when a finished job's result clears, you can use the `keep_result` setting on your worker, see :func:`arq.worker.func`.

.. literalinclude:: examples/job_ids.py

The check of ``job_id`` uniqueness in the queue is performed using a redis transaction so you can be certain jobs
with the same id won't be enqueued twice (or overwritten) even if they're enqueued at exactly the same time.

Job Results
...........

You can access job information, status and job results using the :class:`arq.jobs.Job` instance returned from
:func:`arq.connections.ArqRedis.enqueue_job`.

.. literalinclude:: examples/job_results.py

Retrying jobs and cancellation
..............................

As described above, when an arq worker shuts down, any ongoing jobs are cancelled immediately
(via vanilla ``task.cancel()``, so a ``CancelledError`` will be raised). You can see this by running a slow job
(eg. add ``await asyncio.sleep(5)``) and hitting ``Ctrl+C`` once it's started.

You'll get something like.

.. literalinclude:: examples/slow_job_output.txt

You can also retry jobs by raising the :class:`arq.worker.Retry` exception from within a job,
optionally with a duration to defer rerunning the jobs by:

.. literalinclude:: examples/retry.py

To abort a job, call :func:`arq.job.Job.abort`. (Note for the :func:`arq.job.Job.abort` method to
have any effect, you need to set ``allow_abort_jobs`` to ``True`` on the worker, this is for performance reason.
``allow_abort_jobs=True`` may become the default in future)

:func:`arq.job.Job.abort` will abort a job if it's already running or prevent it being run if it's currently
in the queue.

.. literalinclude:: examples/job_abort.py

Health checks and monitoring
............................

Monitoring and observability tasks remain within the developer's realm of responsibility. However, there are third-party
projects offering monitoring capabilities, such as `arq-ui <https://github.com/antonko/arq-ui>`_. It provides a solution for visualizing and
tracking task execution within arq, albeit with certain limitations.

*arq* will automatically record some info about its current state in redis every ``health_check_interval`` seconds.
That key/value will expire after ``health_check_interval + 1`` seconds so you can be sure if the variable exists *arq*
is alive and kicking (technically you can be sure it was alive and kicking ``health_check_interval`` seconds ago).

You can run a health check with the CLI (assuming you're using the first example above)::

    arq --check demo.WorkerSettings

The command will output the value of the health check if found;
then exit ``0`` if the key was found and ``1`` if it was not.

A health check value takes the following form::

    Mar-01 17:41:22 j_complete=0 j_failed=0 j_retried=0 j_ongoing=0 queued=0

Where the items have the following meaning:

* ``j_complete`` the number of jobs completed
* ``j_failed`` the number of jobs which have failed eg. raised an exception
* ``j_ongoing`` the number of jobs currently being performed
* ``j_retried`` the number of jobs retries run

Cron Jobs
.........

Functions can be scheduled to be run periodically at specific times. See :func:`arq.cron.cron`.

.. literalinclude:: examples/cron.py

Usage roughly shadows `cron <https://helpmanual.io/man8/cron/>`_ except ``None`` is equivalent on ``*`` in crontab.
As per the example sets can be used to run at multiple of the given unit.

Note that ``second`` defaults to ``0`` so you don't in inadvertently run jobs every second and ``microsecond``
defaults to ``123456`` so you don't inadvertently run jobs every microsecond and so *arq* avoids enqueuing jobs
at the top of a second when the world is generally slightly busier.

Synchronous Jobs
................

Functions that can block the loop for extended periods should be run in an executor like
``concurrent.futures.ThreadPoolExecutor`` or ``concurrent.futures.ProcessPoolExecutor`` using
``loop.run_in_executor`` as shown below.

.. literalinclude:: examples/sync_job.py

Custom job serializers
......................

By default, *arq* will use the built-in ``pickle`` module to serialize and deserialize jobs. If you wish to
use an alternative serialization methods, you can do so by specifying them when creating the connection pool
and the worker settings. A serializer function takes a Python object and returns a binary representation
encoded in a ``bytes`` object. A deserializer function, on the other hand, creates Python objects out of
a ``bytes`` sequence.

.. warning::
   It is essential that the serialization functions used by :func:`arq.connections.create_pool` and
   :class:`arq.worker.Worker` are the same, otherwise jobs created by the former cannot be executed by the
   latter. This also applies when you update your serialization functions: you need to ensure that your new
   functions are backward compatible with the old jobs, or that there are no jobs with the older serialization
   scheme in the queue.

Here is an example with `MsgPack <http://msgpack.org>`_, an efficient binary serialization format that
may enable significant memory improvements over pickle:

.. literalinclude:: examples/custom_serialization_msgpack.py


Reference
---------

.. automodule:: arq.connections
   :members:

.. automodule:: arq.worker
   :members: func, Retry, Worker

.. automodule:: arq.cron
   :members: cron

.. automodule:: arq.jobs
   :members: JobStatus, Job

.. include:: ../HISTORY.rst

.. |pypi| image:: https://img.shields.io/pypi/v/arq.svg
   :target: https://pypi.python.org/pypi/arq
.. |license| image:: https://img.shields.io/pypi/l/arq.svg
   :target: https://github.com/samuelcolvin/arq
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _watchfiles: https://pypi.org/project/watchfiles/
.. _rq: http://python-rq.org/
