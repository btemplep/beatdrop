
import datetime
from typing import Callable, List
from unittest.mock import call

from loguru import logger
import pytest
import pytz
import redislite

from beatdrop import entries
from beatdrop.schedulers import Scheduler


@pytest.fixture
def caplog(caplog: pytest.LogCaptureFixture):
    handler_id = logger.add(caplog.handler, format="{message}")
    yield caplog
    logger.remove(handler_id)


@pytest.fixture(scope="function")
def max_interval() -> datetime.timedelta:
    return datetime.timedelta(seconds=1)


@pytest.fixture(scope="function")
def lock_timeout() -> datetime.timedelta:
    return datetime.timedelta(seconds=3)


@pytest.fixture(scope="function")
def test_task() -> str:
    return "beatdrop.validators.dt_is_naive"


@pytest.fixture(scope="function")
def test_args() -> tuple:
    return (
        5,
        6.7,
        "string",
        datetime.datetime.utcnow(),
        pytz.utc.localize(datetime.datetime.utcnow()).astimezone(pytz.timezone("us/eastern"))
    )


@pytest.fixture(scope="function")
def test_kwargs() -> dict:
    return {
        "int": 5,
        "float": 6.7,
        "string": "string",
        "dt_naive": datetime.datetime.utcnow(),
        "dt_aware": pytz.utc.localize(datetime.datetime.utcnow()).astimezone(pytz.timezone("us/eastern"))
    }


@pytest.fixture(scope="function")
def interval_entry(
    test_task: str,
    test_args: list,
    test_kwargs: dict
) -> entries.IntervalEntry:
    return entries.IntervalEntry(
        key="some_interval_due",
        enabled=True,
        task=test_task,
        args=test_args,
        kwargs=test_kwargs,
        period=.1
    )
    

@pytest.fixture(scope="function")
def default_entries(
    test_task: str,
    test_args: list,
    test_kwargs: dict
) -> List[entries.ScheduleEntry]:
    return [
        entries.IntervalEntry(
            key="my_interval",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            period=120
        ),
        entries.IntervalEntry(
            key="my_interval_due",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            period=.1
        ),
        entries.EventEntry(
            key="my_event",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            due_at=datetime.datetime.utcnow() + datetime.timedelta(seconds=120)
        ),
        entries.EventEntry(
            key="my_event_due",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            due_at=datetime.datetime.utcnow()
        ),
        entries.CrontabEntry(
            key="my_cron",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            cron_expression="*/1 * * * *"
        ),
        entries.CrontabEntry(
            key="my_cron_due",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            cron_expression="*/1 * * * *",
            last_sent_at=datetime.datetime.utcnow() - datetime.timedelta(seconds=120)
        ),
        entries.CrontabTZEntry(
            key="my_cron_tz",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            cron_expression="*/1 * * * *",
            timezone="us/eastern"
        ),
        entries.CrontabTZEntry(
            key="my_cron_tz_due",
            enabled=True,
            task=test_task,
            args=test_args,
            kwargs=test_kwargs,
            cron_expression="*/1 * * * *",
            timezone="us/eastern",
            last_sent_at=datetime.datetime.utcnow() - datetime.timedelta(seconds=120)
        )
    ]


@pytest.fixture(scope="function")
def rdb() -> redislite.Redis:
    return redislite.Redis(decode_responses=True)


def run_sched_run_tests(scheduler: Scheduler) -> None:
    scheduler.run(max_iterations=2) 
    print(scheduler.send.call_args_list, flush=True)
    for entry in scheduler.list():
        if entry.key.endswith("due"):
            print(entry, flush=True)
            if call(entry) not in scheduler.send.call_args_list:
                print("ENTRY NOT IN LIST: {}".format(entry))
            assert call(entry) in scheduler.send.call_args_list


@pytest.fixture
def scheduler_run_tests() -> Callable:
    return run_sched_run_tests
