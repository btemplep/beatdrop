
import datetime
from unittest.mock import MagicMock

import pytest
import rq

from beatdrop.entries import IntervalEntry
from beatdrop.schedulers import RQScheduler


@pytest.fixture
def rq_task() -> str:
    def tester_func():
        print("TESTING")
    
    return "tests.unit.rq_scheduler_test.tester_func"


@pytest.fixture(scope="function")
def rq_scheduler(
    rq_queue: rq.Queue,
    max_interval: datetime.timedelta
) -> RQScheduler:
    return RQScheduler(
        max_interval=max_interval,
        rq_queue=rq_queue
    )


@pytest.fixture(scope="function")
def rq_entry(rq_task: str) -> IntervalEntry:
    return IntervalEntry(
        key="my_rq_entry",
        enabled=True,
        task=rq_task,
        period=0.1
    )


def test_creation(
    rq_task: str,
    rq_scheduler: RQScheduler,
) -> None:
    rq_scheduler.rq_queue.is_empty() == True
    rq_scheduler.rq_queue.enqueue(rq_task)
    rq_scheduler.rq_queue.is_empty() == False


def test_send(
    rq_entry: IntervalEntry,
    rq_scheduler: RQScheduler
) -> None:
    assert rq_scheduler.rq_queue.is_empty() == True
    rq_scheduler.send(rq_entry)
    assert rq_scheduler.rq_queue.is_empty() == False
    assert len(rq_scheduler.rq_queue.get_jobs()) == 1
    assert rq_scheduler.rq_queue.get_jobs()[0].func_name == rq_entry.task


def test_send_exception(
    rq_entry: IntervalEntry,
    rq_scheduler: RQScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    rq_scheduler.rq_queue.enqueue = MagicMock(side_effect=[Exception("Bad thing happened")]) 
    rq_scheduler.send(rq_entry)
    assert "ERROR" in caplog.text
    assert "Bad thing happened" in caplog.text
    assert rq_scheduler.rq_queue.is_empty() == True
    assert len(rq_scheduler.rq_queue.get_jobs()) == 0

def test_send_args_kwargs(
    rq_entry: IntervalEntry,
    rq_scheduler: RQScheduler
) -> None:
    rq_entry.args = (5, "string")
    rq_entry.kwargs = {"thing": "here"}
    rq_scheduler.send(rq_entry)
    assert rq_scheduler.rq_queue.is_empty() == False
    assert len(rq_scheduler.rq_queue.get_jobs()) == 1
    job = rq_scheduler.rq_queue.get_jobs()[0]
    assert job.args == rq_entry.args
    assert job.kwargs == rq_entry.kwargs

