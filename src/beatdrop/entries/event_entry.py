
import datetime
from typing import ClassVar, List

import pytz

from beatdrop.entries.schedule_entry import ScheduleEntry


class EventEntry(ScheduleEntry):
    """Event schedules are for a one time event.

    After they run once they are disabled. ``sched.enabled = False``

    Parameters
    ----------
    key : str
        A unique key for the schedule entry.
    enabled : bool
        Enable this entry to be scheduled.
    task : str
        The full python path to the task to run.
    args : Optional[Tuple[Any, ...]]
        Positional arguments to pass the task. 
        These will be serialized/deserialized as JSON. 
        ``jsonpickle`` is used to serialize and deserialize these. 
    kwargs : Optional[Dict[str, Any]]
        Keyword arguments to pass the task. 
        These will be serialized/deserialized as JSON. 
        ``jsonpickle`` is used to serialize and deserialize these.
    due_at : datetime.datetime
        The due at datetime.
        Takes naive or aware datetimes.
        Naive datetimes are assumed to be in UTC.

    Attributes
    ----------
    _logger : ClassVar
        Logger.
    client_read_only_fields : ClassVar[List[str]], optional
        Client read only list of fields. 
        Enumerates the fields that are not normally saved when a client wants to save the entry.
        This is done because the client manages these fields.
        So they are are updated when the scheduler runs them. 
    """

    due_at: datetime.datetime

    client_read_only_fields: ClassVar[List[str]] = ["was_sent"]
    was_sent: bool = False


    def due_in(self) -> datetime.timedelta:
        if self.was_sent:
            # If this entry was already sent just return arbitrarily large timedelta
            return datetime.timedelta(days=1)

        naive_due_at_utc = self.due_at
        if self.due_at.tzinfo is not None:
            print(naive_due_at_utc)
            naive_due_at_utc = self.due_at.astimezone(pytz.utc).replace(tzinfo=None)
            print(naive_due_at_utc)

        return naive_due_at_utc - datetime.datetime.utcnow()

    
    def sent(self):
        self.was_sent = True
        self.enabled = False


    def __str__(self) -> str:
        return "{}(key={}, enabled={}, task={}, args={}, kwargs={}, due_at={})".format(
            type(self).__name__,
            self.key,
            self.enabled,
            self.task,
            self.args,
            self.kwargs,
            self.due_at
        )

