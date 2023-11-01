import asyncio

from arq import create_pool
from arq.connections import RedisSettings
from arq.jobs import Job


async def the_task(ctx):
    print('running the task with id', ctx['job_id'])

async def main():
    redis = await create_pool(RedisSettings())

    # no id, random id will be generated
    job1 = await redis.enqueue_job('the_task')
    print(job1)
    """
    >  <arq job 99edfef86ccf4145b2f64ee160fa3297>
    """

    # random id again, again the job will be enqueued and a job will be returned
    job2 = await redis.enqueue_job('the_task')
    print(job2)
    """
    >  <arq job 7d2163c056e54b62a4d8404921094f05>
    """

    # custom job id, job will be enqueued
    job3 = await redis.enqueue_job('the_task', _job_id='foobar')
    print(job3)
    """
    >  <arq job foobar>
    """

    # same custom job id, job will not be enqueued and enqueue_job will return None
    job4 = await redis.enqueue_job('the_task', _job_id='foobar')
    print(job4)
    """
    >  None
    """

    # you can retrieve jobs by using arq.jobs.Job
    await redis.enqueue_job('the_task', _job_id='my_job')
    job5 = Job(job_id='my_job', redis=redis)
    print(job5)
    """
    <arq job my_job>
    """

class WorkerSettings:
    functions = [the_task]

if __name__ == '__main__':
    asyncio.run(main())
