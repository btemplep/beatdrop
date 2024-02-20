
import datetime

import pytest

from beatdrop.helpers import utc_now_naive
from beatdrop.entries import CrontabEntry


@pytest.fixture
def crontab_entry(
    test_task: str,
    test_args: tuple,
    test_kwargs: dict
) -> CrontabEntry:
    return CrontabEntry(
        key="new_entry",
        enabled=True,
        task=test_task,
        args=test_args,
        kwargs=test_kwargs,
        cron_expression="*/1 * * * *",
        last_sent_at=utc_now_naive() - datetime.timedelta(minutes=3)
    )

def test_creation(crontab_entry: CrontabEntry) -> None:
    crontab_entry.key == "new_entry"


def test_str(crontab_entry: CrontabEntry) -> None:
    entry_str = crontab_entry.__str__()
    assert "CrontabEntry" in entry_str
    assert "key" in entry_str
    assert "enabled" in entry_str
    assert "task" in entry_str
    assert "args" in entry_str
    assert "kwargs" in entry_str
    assert "cron_expression" in entry_str


def test_due_in(crontab_entry: CrontabEntry) -> None:
    assert crontab_entry.due_in().total_seconds() < 0


def test_sent(crontab_entry: CrontabEntry) -> None:
    last_sent = crontab_entry.last_sent_at
    crontab_entry.sent()
    assert last_sent < crontab_entry.last_sent_at
