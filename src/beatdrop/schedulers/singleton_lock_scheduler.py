
import datetime
from typing import Optional

from pydantic.dataclasses import dataclass
from pydantic import root_validator

from beatdrop.schedulers.scheduler import Scheduler
from beatdrop import exceptions


@dataclass
class SingletonLockScheduler(Scheduler):
    """Base singleton lock scheduler class.

    This base class is used by scheduler where more that one can be started in parallel with ``run``,
    but only one *should* be checking entries and sending them. Uses a lock based approach to 
    stop/tell other schedulers from running and sending tasks.

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
    """

    lock_timeout: datetime.timedelta
   

    @root_validator
    def lock_timeout_3_times_interval(cls, values: dict) -> dict:
        if values['lock_timeout'] / values['max_interval'] < 3:
            raise ValueError("'lock_timeout' must be at least 3 times as long as the `max_interval`.")
        
        return values
