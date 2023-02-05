
import datetime
from typing import Iterator, List, Optional, Tuple, Type, Union

from pydantic.dataclasses import dataclass
from pydantic import Field, validator

from beatdrop.logger import logger
from beatdrop.entry_type_registry import EntryTypeRegistry
from beatdrop.exceptions import \
    MaxRunIterations, \
    MethodNotImplementedError, \
    OverwriteDefaultEntryError
from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop import entries, messages


@dataclass(kw_only=True)
class Scheduler:
    """Base Scheduler class.

    All runnable schedulers **must** implement these methods:

    - ``run`` - Run the scheduler. 
    - ``send`` - Send a schedule entry to the task system. 

    All schedulers *should* implement these methods :

    - ``list`` - List schedule entries.
    - ``get`` - Get a schedule entry.
    - ``save`` - Save a new or update an existing schedule entry.
    - ``delete`` - Delete a schedule entry.

    See the docs on the methods for more information.

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

    max_interval: datetime.timedelta
    sched_entry_types: Tuple[Type[ScheduleEntry]] = Field(
        default=(
            entries.CrontabEntry,
            entries.CrontabTZEntry,
            entries.EventEntry,
            entries.IntervalEntry
        )
    )
    default_sched_entries: Optional[List[ScheduleEntry]] = Field(default=[])


    def __post_init_post_parse__(self):
       self._logger = logger
       self._entry_type_registry = EntryTypeRegistry(sched_entry_types=self.sched_entry_types)
       self._default_sched_entry_lookup = {entry.key: entry for entry in self.default_sched_entries}


    def run(self, max_iterations: int = None) -> None:
        """Run the scheduler.

        Parameters
        ----------
        max_iterations: int 
            default : None

            The maximum number of iterations to run the scheduler.
            None is unlimited.
        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            Must implement ``run`` method.
        """
        raise MethodNotImplementedError("The 'run' method must be implemented in a scheduler")

    
    def send(self, sched_entry: ScheduleEntry) -> None: 
        """Send a schedule entry to the task backend.

        This should be used by the ``run`` method when a schedule is due.
        Subclasses can override this for common schedulers without changing the specifics of how a schedule runs.
        Send it to a queue, to celery etc.

        **NOTE**: for the reasons above the ``send`` method should not perform any actions against the 
        state of the scheduler or schedule entries.  

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Schedule entry that will be sent to the task backend.

        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            Must implement ``send`` method.
        """
        raise MethodNotImplementedError("Must implement the 'send' method for a scheduler.")


    def list(self) -> Iterator[ScheduleEntry]:
        """List schedule entries.

        Returns
        -------
        Iterator[ScheduleEntry]
            List of schedule entries.

        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            ``list`` method not implemented.
        """
        raise MethodNotImplementedError("This scheduler does not support retrieving entries or has not implemented it.")


    def get(self, key: str) -> ScheduleEntry:
        """Retrieve a schedule entry by its key.

        Parameters
        ----------
        key : str
            The schedule entry key.

        Returns
        -------
        ScheduleEntry
            The schedule entry with the matching key.

        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            ``get`` method not implemented.
        """
        raise MethodNotImplementedError("This scheduler does not support retrieving entries or has not implemented it.")
    

    def save(
        self, 
        sched_entry: ScheduleEntry,
        client_read_only: bool = False
    ) -> None:
        """Save a new or update an existing schedule entry.

        By default, read only attributes are not updated with ``save`` (suffixed with ``__``).

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Schedule entry to add or update in scheduler.
        client_read_only: bool
            Overwrite client read only fields?  
            ``False`` **will not** overwrite client read only fields.  
            ``True`` will.   
            This should almost always be ``False``, unless you are a scheduler, or you know what you're doing.

        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            ``save`` method not implemented.
        """
        raise MethodNotImplementedError("This scheduler does not support saving entries or has not implemented it.")


    def delete(self, sched_entry: ScheduleEntry) -> None:
        """Delete a schedule entry from the scheduler.

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Scheduler entry to delete from the scheduler.

        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            ``delete`` method not implemented.
        """
        raise MethodNotImplementedError("This scheduler does not support deleting entries or has not implemented it.")

    
    def _update_run_iteration(
        self,
        num_iterations: int,
        max_iterations: Union[int, None]
    ) -> int:
        """Helper for ``run`` method to update its run iterations

        Parameters
        ----------
        num_iterations : int
            Current number or iterations for ``run``
        max_iterations : Union[int, None]
            Max desired iterations for ``run``, or None for unlimited.

        Returns
        -------
        int
            Current number of iterations.

        Raises
        ------
        MaxRunIterations
            _description_
        """
        if max_iterations is not None:
            num_iterations += 1
            if num_iterations >= max_iterations:
                self._logger.info(messages.scheduler_max_iterations)
                raise MaxRunIterations(messages.scheduler_max_iterations)
        
        return num_iterations


    def _check_default_entry_overwrite(self, sched_entry: ScheduleEntry) -> None:
        if sched_entry.key in self._default_sched_entry_lookup:
            raise OverwriteDefaultEntryError(
                "The schedule entry key '{}' is in default entries and cannot be overwritten.".format(sched_entry.key)
            )


    @validator("max_interval")
    def max_interval_gte_one(
        cls,
        max_interval: datetime.timedelta
    ) -> datetime.timedelta:
        if max_interval.total_seconds() < 1:
            raise ValueError("max_interval must be greater than or equal to 1 second.")
        
        return max_interval


