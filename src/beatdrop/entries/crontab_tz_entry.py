
import datetime
from typing import ClassVar, List

from croniter import croniter
from pydantic import Field, validator
import pytz

from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop import validators


class CrontabTZEntry(ScheduleEntry):
    """Crontab style schedule entry based on a specific timezone.

    Useful if you want a schedule entry to be sent during the same time in DST timezones.
    
    For example, we want an entry to be sent at 1pm US/Eastern. 
    With the normal ``CrontabEntry``, we could set the ``cron_expression`` to ``"0 17 * * *"`` in the summer,
    but then in the winter the entry is now sent at 2pm US/Eastern instead. 

    The ``CrontabTZEntry`` solves this by setting the ``cron_expression`` to ``"0 13 * * *"`` 
    and the ``timezone`` to ``"US/Eastern"``. 
    The schedule entry is due at 1pm, US/Eastern every day.

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
    cron_expression : str
        Crontab style date and time expression.
        ``croniter`` package is currently used as the parser. 
        https://pypi.org/project/croniter/
    timezone : str
        The timezone string.
        ``pytz`` library is used to parse this and create aware datetimes.
    last_sent_at : datetime.datetime, optional
        **Client read only field**
        Last time the entry was sent.  Naive datetime in UTC.

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
   
    cron_expression: str
    timezone: str

    client_read_only_fields: ClassVar[List[str]] = ["last_sent_at"]
    last_sent_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)

    _dt_is_naive = validator(
        "last_sent_at",
        allow_reuse=True
    )(validators.dt_is_naive)

    _valid_cron_expressions = validator(
        "cron_expression",
        allow_reuse=True
    )(validators.valid_cron_expression)


    def due_in(self) -> datetime.timedelta:
        # if we keep last_sent at as a naive utc
        timezone = pytz.timezone(self.timezone)
        crony = croniter(
            expr_format=self.cron_expression, 
            start_time=pytz.utc.localize(self.last_sent_at).astimezone(timezone),
            ret_type=datetime.datetime
        )

        return crony.get_next().astimezone(pytz.utc).replace(tzinfo=None) - datetime.datetime.utcnow()

    
    def sent(self): 
        self.last_sent_at = datetime.datetime.utcnow()


    def __str__(self) -> str:
        # return "{}(key={})".format(
        #     type(self).__name__,
        #     self.key
        # )
        return "{}(key={}, enabled={}, task={}, args={}, kwargs={}, cron_expression={}, timezone={})".format(
            type(self).__name__,
            self.key,
            self.enabled,
            self.task,
            self.args,
            self.kwargs,
            self.cron_expression,
            self.timezone
        )

    @validator(
        "timezone"
    )
    def valid_timezone(cls, v):
        pytz.timezone(v)

        return v


