Quickstart
==========


There are 2 main pieces to using `beatdrop`.

- Schedule Entry - Hold the task definition along with scheduling info.

- Schedulers - These perform 2 main roles.  
    - They can be run as a scheduler ie monitor and send tasks to the task backend.
    - Act as clients for reading and writing schedule entries.


.. code-block:: python

    from celery import Celery
    from beatdrop import CeleryRedisScheduler, IntervalEntry


    celery_app = Celery()

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