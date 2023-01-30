
from importlib import import_module
import pathlib
from typing import Any

from pydantic.dataclasses import dataclass

from beatdrop import messages
from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop.schedulers.scheduler import Scheduler


@dataclass
class CeleryScheduler(Scheduler):
    """Implementation for sending celery tasks.

    Combine as a second base class to be able to send tasks to Celery queues.

    Example:

    .. code-block:: python

        from beatdrop.schedulers import CeleryScheduler, SQLScheduler

        class CelerySQLScheduler(SQLScheduler, CeleryScheduler):
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
    celery_app : celery.Celery
        Celery app for sending tasks.
    """

    celery_app: Any


    def send(self, sched_entry: ScheduleEntry) -> None:
        """Send a schedule entry to the Celery queue.

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Schedule entry to send to the Celery queue.
        """
        self._logger.debug(messages.sched_entry_sending_template.format(sched_entry))
        try:
            task_name = sched_entry.task
            if task_name.startswith("__main__"):
                main_module = import_module("__main__")
                task_name = sched_entry.task.replace(
                    "__main__", 
                    pathlib.Path(main_module.__file__).name.split(".")[0]
                )
            
            task_args = sched_entry.args
            task_kwargs = sched_entry.kwargs
            if task_args is None:
                task_args = []
            
            if task_kwargs is None:
                task_kwargs = {}
            
            if task_name in self.celery_app.tasks:
                self.celery_app.tasks[task_name].delay(*task_args, **task_kwargs)
                self._logger.info(messages.sched_entry_sent_template.format(sched_entry))
            else:
                self._logger.error("Could not find Celery task {} for entry {}".format(task_name, sched_entry))

        except Exception as error:
            self._logger.error(
                "Failed to send entry: {}. Check that the Celery app is initialized and the function is registered as a task. {}: {}".format(
                    sched_entry,
                    type(error).__name__, 
                    error
                )
            )



