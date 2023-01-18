
from typing import Callable, List
from unittest.mock import call, MagicMock

import pytest

from beatdrop import entries
from beatdrop.schedulers import MemScheduler


@pytest.fixture
def mem_scheduler(default_entries: List[entries.ScheduleEntry]) -> MemScheduler:
    mem_sched =  MemScheduler(
        max_interval=60,
        default_sched_entries=default_entries
    )
    mem_sched.send = MagicMock(return_value=None)

    return mem_sched


def test_creation(mem_scheduler: MemScheduler) -> None:
    assert mem_scheduler.max_interval.total_seconds() == 60


def test_run(
    mem_scheduler: MemScheduler,
    scheduler_run_tests: Callable
) -> None:
    scheduler_run_tests(mem_scheduler)


