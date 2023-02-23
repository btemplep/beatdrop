# `beatdrop`

![](https://github.com/btemplep/beatdrop/raw/main/docs/_static/beatdrop_logo.svg)

See the full [Documentation](https://docs.pythonbeatdrop.com/).

The goal of `beatdrop` is to provide schedulers and schedule entries that are easy to use, extensible, scalable, and backend agnostic. 

It **does not** run tasks or python functions on a schedule. It will simply interface with task backends to send tasks when they are due.


## Installation

Install the base package with pip from [PyPi](https://pypi.org/project/beatdrop/).

```text
$ pip install beatdrop
```

For particular schedulers and backends you will also need to install their extra dependencies.

```text
$ pip install beatdrop[redis]
```

Extra dependencies for task backends:

- `celery` 

Extra dependencies for scheduler storage:

- `redis`

- `sql`

The `all` extra dependency will install all extra dependencies for task backends and scheduler storage.

```text
$ pip install beatdrop[all]
```

## Usage

There are 2 main pieces to using ``beatdrop``.

- Schedule Entry - holds the task definitions and scheduling info.

- Schedulers - have 2 main roles 
    - They can be run as a scheduler to monitor and send tasks to the task backend.
    - Act as clients for reading and writing schedule entries.

To run the scheduler simply make a python file, create the scheduler and call the run method:

```python
from beatdrop import CeleryRedisScheduler

from my_app import celery_app


sched = CeleryRedisScheduler(
    max_interval=60,
    celery_app=celery_app,
    lock_timeout=180,
    redis_py_kwargs={
        "host": "my.redis.host",
        "port": 6379,
        "db": 0,
        "password": "mys3cr3t"
    }
)
sched.run()
```


To use the scheduler as a client, you create the scheduler the same as you would to run it:

```python
from beatdrop import CeleryRedisScheduler, IntervalEntry

from my_app import celery_app


# Create a scheduler
sched = CeleryRedisScheduler(
    max_interval=60,
    celery_app=celery_app,
    lock_timeout=180,
    redis_py_kwargs={
        "host": "my.redis.host",
        "port": 6379,
        "db": 0,
        "password": "mys3cr3t"
    }
)
# create a schedule entry
inter = IntervalEntry(
    key="my-interval-entry",
    enabled=True,
    task="test_task",
    args=("my_args", 123),
    kwargs={
        "my_kwargs": 12.4
    },
    period=10
)

# save or update an entry 
sched.save(inter)
# list all entries, this will automatically paginate
schedule_entries = sched.list()
# retrieve a specific entry
my_inter_entry = sched.get(inter.key)
# equivalent to the line above
my_inter_entry = sched.get("my-interval-entry")
# Delete an entry from the scheduler
sched.delete(inter)
```
