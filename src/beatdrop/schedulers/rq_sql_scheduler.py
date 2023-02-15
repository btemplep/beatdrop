

from pydantic.dataclasses import dataclass

from beatdrop.schedulers.rq_scheduler import RQScheduler
from beatdrop.schedulers.sql_scheduler import SQLScheduler


class Config:
    arbitrary_types_allowed = True

@dataclass(config=Config)
class RQSQLScheduler(SQLScheduler, RQScheduler):
    """Hold schedule entries in an SQL database, and send to RQ (Redis Queue) task queues.

    Uses an SQL database to store schedule entries and scheduler state.
    It is safe to run multiple ``RQSQLScheduler`` s simultaneously, 
    as well as have many that are purely used as clients to read/write entries.

    **NOTE** - You must also install the DB driver specified in the URL for ``create_engine_kwargs``.

    **NOTE** - Before running the scheduler for the first time the 
    DB tables must be created using ``create_tables()``.


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
    create_engine_kwargs: dict
        Keyword arguments to pass to ``sqlalchemy.create_engine``.
        See SQLAlchemy docs for more info. 
        https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine
    rq_queue : rq.Queue
        RQ Queue to send tasks to.

    Example
    -------
    .. code-block:: python

        from rq import Queue
        from beatdrop import RQSQLScheduler

        rq_queue = Queue()
        sched = RQSQLScheduler(
            max_interval=60,
            rq_queue=rq_queue,
            lock_timeout=180,
            create_engine_kwargs={
                "url": "sqlite:///my_sqlite.db"
            }
        )
        # use the scheduler as a client
        entry_list = sched.list()
        for entry in entry_list:
            print(entry.key)

        # or run it
        sched.run()
    """