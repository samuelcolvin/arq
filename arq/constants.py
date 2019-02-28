queue_name = 'arq:queue'
job_key_prefix = 'arq:job:'
in_progress_key_prefix = 'arq:in-progress:'
result_key_prefix = 'arq:result:'
retry_key_prefix = 'arq:retry:'
cron_key_prefix = 'arq:cron:'

default_timeout = 300_000
default_max_jobs = 10
default_keep_result = 3_600_000
default_max_tries = 5
