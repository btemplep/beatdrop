
import copy
import time
from datetime import timedelta

from pydantic.dataclasses import dataclass

from beatdrop import art, messages
from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop.exceptions import MaxRunIterations
from beatdrop.schedulers.scheduler import Scheduler


@dataclass
class MemScheduler(Scheduler):
    """Static in memory scheduler.

    This scheduler does not implement the ``send`` method.
    This must be implemented before it can actually send tasks
    to the specified backend.

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
    """


    def run(self, max_iterations: int = None) -> None:
        """Run the scheduler.

        Parameters
        ----------
        max_iterations: int 
            default : None

            The maximum number of iterations to run the scheduler.
            None is unlimited.
        """
        try:
            self._logger.info(art.logo)
            self._logger.info(messages.scheduler_starting)
            zero = timedelta(seconds=0)
            sleep_time = self.max_interval
            num_iterations = 0
            while True:
                for entry in self.default_sched_entries:
                    if not entry.enabled:
                        continue

                    due_in = entry.due_in()
                    if due_in <= zero:
                        self._logger.debug(messages.sched_entry_sending_template.format(entry))
                        self.send(entry)
                        entry.sent()
                        due_in = entry.due_in()

                    if due_in < sleep_time:
                        sleep_time = due_in

                num_iterations = self._update_run_iteration(
                    num_iterations=num_iterations, 
                    max_iterations=max_iterations
                )
                self._logger.debug(messages.scheduler_sleep_template.format(sleep_time.total_seconds()))
                time.sleep(sleep_time.total_seconds())
                sleep_time = self.max_interval
        except (MaxRunIterations, KeyboardInterrupt):
            self._logger.info(messages.scheduler_shut_down)


    def list(self) -> ScheduleEntry:
        return copy.deepcopy(self.default_sched_entries)

