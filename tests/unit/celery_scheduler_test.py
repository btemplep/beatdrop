
import datetime
import json
from unittest.mock import MagicMock

import celery
import pytest
import redislite

from beatdrop import messages
from beatdrop.entries import IntervalEntry
from beatdrop.schedulers import CeleryScheduler


@pytest.fixture(scope="function")
def celery_app(rdb: redislite.Redis) -> celery.Celery:
    return celery.Celery("tester", broker="redis+socket://{}?db=0".format(rdb.socket_file))


@pytest.fixture
def celery_task(celery_app: celery.Celery) -> str:
    @celery_app.task
    def tester_func():
        print("TESTING")

    return "tests.unit.celery_scheduler_test.tester_func"


@pytest.fixture(scope="function")
def celery_scheduler(
    celery_app: celery.Celery,
    max_interval: datetime.timedelta
) -> CeleryScheduler:
    return CeleryScheduler(
        max_interval=max_interval,
        celery_app=celery_app
    )

@pytest.fixture(scope="function")
def celery_entry(
    celery_task: str
) -> IntervalEntry:
    return IntervalEntry(
        key="my_celery_entry",
        enabled=True,
        task=celery_task,
        period=.1
    )


def test_creation(
    celery_task: str,
    celery_scheduler: CeleryScheduler,
) -> None:
    assert len(celery_scheduler.celery_app.tasks) > 0
    assert celery_task in celery_scheduler.celery_app.tasks


def test_send(
    rdb: redislite.Redis,
    celery_entry: IntervalEntry,
    celery_scheduler: CeleryScheduler
) -> None:
    celery_scheduler.send(celery_entry)
    assert len(rdb.lrange("celery", 0, 100)) == 1
    assert json.loads(rdb.lrange("celery", 0, 100)[0])['headers']['task'] == celery_entry.task


def test_send_not_found(
    rdb: redislite.Redis,
    celery_entry: IntervalEntry,
    celery_scheduler: CeleryScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    celery_entry.task = "thing.not.found"
    celery_scheduler.send(celery_entry)
    assert "ERROR" in caplog.text
    assert celery_entry.task in caplog.text
    assert len(rdb.lrange("celery", 0, 100)) == 0


def test_send_main_task(
    rdb: redislite.Redis,
    celery_entry: IntervalEntry,
    celery_scheduler: CeleryScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    celery_entry.task = "__main__.some.task"
    celery_scheduler.send(celery_entry)
    assert "ERROR" in caplog.text
    assert "pytest." in caplog.text # Pytest runs from pytest.py so the task should start with "pytest."
    assert len(rdb.lrange("celery", 0, 100)) == 0


def test_send_exception(
    rdb: redislite.Redis,
    celery_entry: IntervalEntry,
    celery_scheduler: CeleryScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    celery_scheduler.celery_app.tasks[celery_entry.task].delay = MagicMock(side_effect=[Exception]) 
    celery_scheduler.send(celery_entry)
    assert "ERROR" in caplog.text
    assert len(rdb.lrange("celery", 0, 100)) == 0

