
import datetime
import time

import pytest
import pytz

from beatdrop.entries import EventEntry
from beatdrop.helpers import utc_now_naive

@pytest.fixture(
    params=[False, True],
    ids=["due_at_naive", "due_at_aware"]
)
def event_entry(
    request, 
    test_task: str,
    test_args: tuple,
    test_kwargs: dict
) -> EventEntry:
    if request.param:
        due_at = pytz.utc.localize(utc_now_naive()).astimezone(pytz.timezone("us/eastern"))
    else:
        due_at = utc_now_naive()

    return EventEntry(
        key="new_entry",
        enabled=True,
        task=test_task,
        args=test_args,
        kwargs=test_kwargs,
        due_at=due_at
    )


def test_creation(event_entry: EventEntry) -> None:
    event_entry.key == "new_entry"


def test_str(event_entry: EventEntry) -> None:
    entry_str = event_entry.__str__()
    assert "EventEntry" in entry_str
    assert "key" in entry_str
    assert "enabled" in entry_str
    assert "task" in entry_str
    assert "args" in entry_str
    assert "kwargs" in entry_str
    assert "due_at" in entry_str


def test_due_in(event_entry: EventEntry) -> None:
    assert event_entry.due_in().total_seconds() < 0
    assert event_entry.due_in().total_seconds() > -1


def test_sent(event_entry: EventEntry) -> None:
    event_entry.sent()
    assert event_entry.was_sent == True
    assert event_entry.due_in().total_seconds() > 500

