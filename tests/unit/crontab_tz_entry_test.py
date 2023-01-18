
import datetime

import pytest

from beatdrop.entries import CrontabTZEntry


@pytest.fixture
def crontab_tz_entry(
    test_task: str,
    test_args: tuple,
    test_kwargs: dict
) -> CrontabTZEntry:
    return CrontabTZEntry(
        key="new_entry",
        enabled=True,
        task=test_task,
        args=test_args,
        kwargs=test_kwargs,
        cron_expression="*/1 * * * *",
        timezone="us/eastern",
        last_sent_at=datetime.datetime.utcnow() - datetime.timedelta(minutes=3)
    )

def test_creation(crontab_tz_entry: CrontabTZEntry) -> None:
    crontab_tz_entry.key == "new_entry"


def test_str(crontab_tz_entry: CrontabTZEntry) -> None:
    entry_str = crontab_tz_entry.__str__()
    assert "CrontabTZEntry" in entry_str
    assert "key" in entry_str
    assert "enabled" in entry_str
    assert "task" in entry_str
    assert "args" in entry_str
    assert "kwargs" in entry_str
    assert "cron_expression" in entry_str
    assert "timezone" in entry_str


def test_due_in(crontab_tz_entry: CrontabTZEntry) -> None:
    assert crontab_tz_entry.due_in().total_seconds() < 0


def test_sent(crontab_tz_entry: CrontabTZEntry) -> None:
    last_sent = crontab_tz_entry.last_sent_at
    crontab_tz_entry.sent()
    assert last_sent < crontab_tz_entry.last_sent_at
