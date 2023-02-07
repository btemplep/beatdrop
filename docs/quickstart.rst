Quickstart
==========


There are 2 main pieces to using ``beatdrop``.

- Schedule Entry - holds the task definitions and scheduling info.

- Schedulers - have 2 main roles 
    - They can be run as a scheduler to monitor and send tasks to the task backend.
    - Act as clients for reading and writing schedule entries.

To run the scheduler simply make a python file, create the scheduler and call the run method:

.. code-block:: python

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


To use the scheduler as a client, you create the scheduler the same as you would to run it:

.. code-block:: python

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
