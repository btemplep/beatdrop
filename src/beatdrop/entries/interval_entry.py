
import datetime
from typing import ClassVar, List

from pydantic import Field, validator

from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop import validators


class IntervalEntry(ScheduleEntry):
    """Interval schedule entries are sent every ``period`` amount of time.

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
    period : datetime.timedelta
        How often to run the schedule entry.
    last_sent_at : datetime.datetime, optional
        **Client read only field**
        Last time the entry was sent

    Attributes
    ----------
    _logger : ClassVar
        Logger.
    client_read_only_fields : ClassVar[List[str]] = []
        Client read only list of fields. 
        Enumerates the fields that are not normally saved when a client wants to save the entry.
        This is done because the client manages these fields.
        So they are are updated when the scheduler runs them. 
    """
   
    period: datetime.timedelta

    client_read_only_fields: ClassVar[List[str]] = ["last_sent_at"]
    last_sent_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    _dt_is_naive = validator(
        "last_sent_at",
        allow_reuse=True
    )(validators.dt_is_naive)

    def due_in(self) -> datetime.timedelta:
        utc_now = datetime.datetime.utcnow()
        delta_since_sent = utc_now - self.last_sent_at
        
        return self.period - delta_since_sent

    
    def sent(self):
        self.last_sent_at = datetime.datetime.utcnow()


    def __str__(self) -> str:
        return "{}(key={}, enabled={}, task={}, args={}, kwargs={}, period={})".format(
            type(self).__name__,
            self.key,
            self.enabled,
            self.task,
            self.args,
            self.kwargs,
            self.period
        )

    @validator(
        "period"
    )
    def timedelta_positive(
        cls,
        v: datetime.timedelta
    ) -> datetime.timedelta:
        if v.total_seconds() <= 0:
            raise ValueError("datetime.timedelta objects must be positive")

        return v

 