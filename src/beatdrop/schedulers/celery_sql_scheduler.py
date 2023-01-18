

from pydantic.dataclasses import dataclass

from beatdrop.schedulers.celery_scheduler import CeleryScheduler
from beatdrop.schedulers.sql_scheduler import SQLScheduler


@dataclass
class CelerySQLScheduler(SQLScheduler, CeleryScheduler):
    """Hold scheduler entries in an SQL database, and send tasks to Celery queues.

    Uses an SQL database to store schedule entries and scheduler state.
    It is safe to run multiple ``SQLScheduler``s simultaneously, 
    as well as have many that are purely used as clients to read/write entries.

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
    lock_timeout: datetime.timedelta
        The time a scheduler does not refresh the scheduler lock before it is considered dead. 
        Should be at least 3 times the ``max_interval``.
    create_engine_kwargs: dict
        Keyword arguments to pass to ``sqlalchemy.create_engine``.
        See SQLAlchemy docs for more info. 
        https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine
    """


    