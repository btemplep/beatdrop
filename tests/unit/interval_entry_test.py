
import datetime

import pytest
import pytz

from beatdrop.helpers import utc_now_naive
from beatdrop.entries import IntervalEntry

period = 30

@pytest.fixture
def interval_entry(
    test_task: str,
    test_args: tuple,
    test_kwargs: dict
) -> IntervalEntry:
    return IntervalEntry(
        key="new_entry",
        enabled=True,
        task=test_task,
        args=test_args,
        kwargs=test_kwargs,
        period=period
    )


def test_creation(interval_entry: IntervalEntry) -> None:
    interval_entry.key == "new_entry"


def test_bad_timedelta() -> None:
    with pytest.raises(ValueError):
        IntervalEntry(
            key="new_entry",
            enabled=True,
            period=0
        )

    with pytest.raises(ValueError):
        IntervalEntry(
            key="new_entry",
            enabled=True,
            period=-1
        )


def test_str(interval_entry: IntervalEntry) -> None:
    entry_str = interval_entry.__str__()
    assert "IntervalEntry" in entry_str
    assert "key" in entry_str
    assert "enabled" in entry_str
    assert "task" in entry_str
    assert "args" in entry_str
    assert "kwargs" in entry_str
    assert "period" in entry_str


def test_due_in(interval_entry: IntervalEntry) -> None:
    assert interval_entry.due_in().total_seconds() < period


def test_sent(interval_entry: IntervalEntry) -> None:
    last_sent = interval_entry.last_sent_at
    interval_entry.sent()
    assert last_sent < interval_entry.last_sent_at


def test_bad_last_sent_at(interval_entry: IntervalEntry) -> None:
    with pytest.raises(ValueError):
        interval_entry.last_sent_at = pytz.utc.localize(utc_now_naive()).astimezone(pytz.timezone("us/eastern"))

