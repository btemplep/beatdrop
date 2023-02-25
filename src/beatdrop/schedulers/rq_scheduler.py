
from typing import Any

import rq
from pydantic import Field
from pydantic.dataclasses import dataclass

from beatdrop import messages
from beatdrop.schedulers.scheduler import Scheduler
from beatdrop.entries.schedule_entry import ScheduleEntry


class Config:
    arbitrary_types_allowed = True

@dataclass(config=Config)
class RQScheduler(Scheduler):
    """Implementation for sending RQ (Redis Queue) tasks.

    Combine as a second base class to be able to send tasks to RQ queues.

    Example:

    .. code-block:: python

        from beatdrop.schedulers import RQScheduler, SQLScheduler

        class RQSQLScheduler(SQLScheduler, RQScheduler):
            pass

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
    rq_queue : rq.Queue
        RQ Queue to send tasks to.
    """

    rq_queue: rq.Queue = Field()


    def send(self, sched_entry: ScheduleEntry) -> None:
        """Send a schedule entry to the RQ queue.

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Schedule entry to send to the RQ queue.
        """
        try:
            self._logger.debug(messages.sched_entry_sending_template.format(sched_entry))
            task_args = sched_entry.args
            task_kwargs = sched_entry.kwargs
            if task_args is None:
                task_args = []
            
            if task_kwargs is None:
                task_kwargs = {}
            
            self.rq_queue.enqueue(sched_entry.task, args=task_args, kwargs=task_kwargs)
            self._logger.info(messages.sched_entry_sent_template.format(sched_entry))
        except Exception as error:
            self._logger.error(
                "Failed to send entry: {}. {}: {}".format(
                    sched_entry,
                    type(error).__name__, 
                    error
                )
            )
