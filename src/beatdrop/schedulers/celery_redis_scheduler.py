

from pydantic.dataclasses import dataclass

from beatdrop.schedulers.celery_scheduler import CeleryScheduler
from beatdrop.schedulers.redis_scheduler import RedisScheduler


@dataclass
class CeleryRedisScheduler(RedisScheduler, CeleryScheduler):
    """Hold schedule entries in Redis, and send to Celery task queues.

    Uses Redis to store schedule entries and scheduler state.
    It is safe to run multiple ``CeleryRedisScheduler`` s simultaneously, 
    as well as have many that are used as clients to read/write entries.

    Parameters
    ----------
    max_interval : datetime.timedelta
        The maximum interval that the scheduler should sleep before waking up to check for due tasks.
    sched_entry_types : Tuple[Type[ScheduleEntry]], default : (CrontabEntry, CrontabTZEntry, EventEntry, IntervalEntry)
        A list of valid schedule entry types for this scheduler.
        These are only stored in the scheduler, not externally.
    default_sched_entries : List[ScheduleEntry], default : []
        Default list of schedule entries.  
        In general these entries are not held in non-volatile storage 
        so any metadata they hold will be lost if the scheduler fails.
        These entries are static.  The keys cannot be overwritten or deleted.
    lock_timeout : datetime.timedelta
        The time a scheduler does not refresh the scheduler lock before it is considered dead. 
        Should be at least 3 times the ``max_interval``.
    redis_py_kwargs : Dict[str, Any]
        redis-py's ``redis.Redis()`` key word arguments. Some of the client configuration items may be overwritten.
        https://redis-py.readthedocs.io/en/stable/connections.html#generic-client
    celery_app : celery.Celery
        Celery app for sending tasks.

    Example
    -------
    .. code-block:: python

        from celery import Celery
        from beatdrop import CeleryRedisScheduler

        celery_app = Celery()
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
        # use the scheduler as a client
        entry_list = sched.list()
        for entry in entry_list:
            print(entry.key)

        # or run it
        sched.run()
    """


    