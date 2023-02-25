
import datetime
from pathlib import Path
from typing import Callable, List
from unittest.mock import call, MagicMock

import celery
from loguru import logger
import pytest
import pytz
from redis import Redis
import redislite
import rq

from beatdrop import entries
from beatdrop.schedulers import Scheduler
from beatdrop import \
    ScheduleEntry, \
    RedisScheduler, \
    SQLScheduler


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


@pytest.fixture(scope="function")
def celery_app(rdb: redislite.Redis) -> celery.Celery:
    return celery.Celery("tester", broker="redis+socket://{}?db=0".format(rdb.socket_file))


@pytest.fixture(scope="function")
def redis_scheduler(
    max_interval: datetime.timedelta,
    lock_timeout: datetime.timedelta,
    default_entries: List[ScheduleEntry],
    rdb: Redis
) -> RedisScheduler:
    redis_sched =  RedisScheduler(
        max_interval=max_interval,
        default_sched_entries=default_entries,
        lock_timeout=lock_timeout,
        redis_py_kwargs={
            "unix_socket_path": rdb.socket_file
        }
    )
    redis_sched.send = MagicMock(return_value=None)

    return redis_sched


@pytest.fixture
def sql_scheduler(
    max_interval: datetime.timedelta,
    lock_timeout: datetime.timedelta,
    default_entries: List[entries.ScheduleEntry]
) -> SQLScheduler:
    test_db_path = Path("./unit_test.sqlite").resolve()
    sql_sched =  SQLScheduler(
        max_interval=max_interval,
        default_sched_entries=default_entries,
        lock_timeout=lock_timeout,
        create_engine_kwargs={
            "url": "sqlite:///{}".format(test_db_path)
        }
    )
    sql_sched.send = MagicMock(return_value=None)
    sql_sched.create_tables()

    yield sql_sched

    test_db_path.unlink()


@pytest.fixture(scope="function")
def rq_queue(rdb: redislite.Redis) -> rq.Queue:
    return rq.Queue(
        connection=Redis(
            unix_socket_path=rdb.socket_file,
            db=0
        )
    )

