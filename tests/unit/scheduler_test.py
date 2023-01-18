
from typing import List

import pytest

from beatdrop import entries, exceptions
from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop.schedulers import Scheduler


@pytest.fixture
def scheduler(default_entries: List[entries.ScheduleEntry]) -> Scheduler:
    return Scheduler(
        max_interval=30,
        default_sched_entries=default_entries
    )


def test_creation(scheduler: Scheduler) -> None:
    assert len(scheduler.default_sched_entries) == 8


def test_bad_max_interval() -> None:
    with pytest.raises(ValueError):
        Scheduler(
            max_interval=0
        )

    with pytest.raises(ValueError):
        Scheduler(
            max_interval=-1
        )


def test_not_implemented_methods(scheduler: ScheduleEntry) -> None:
    with pytest.raises(exceptions.MethodNotImplementedError):
        scheduler.run()

    with pytest.raises(exceptions.MethodNotImplementedError):
        scheduler.send("test")

    with pytest.raises(exceptions.MethodNotImplementedError):
        scheduler.list()

    with pytest.raises(exceptions.MethodNotImplementedError):
        scheduler.get("test")

    with pytest.raises(exceptions.MethodNotImplementedError):
        scheduler.save("test")

    with pytest.raises(exceptions.MethodNotImplementedError):
        scheduler.delete("test")


def test__update_run_iteration(scheduler: Scheduler) -> None:
    num_iters = 0
    max_iters = None
    num_iters = scheduler._update_run_iteration(
        num_iterations=num_iters,
        max_iterations=max_iters
    )
    assert num_iters == 0

    max_iters = 2
    num_iters = scheduler._update_run_iteration(
        num_iterations=num_iters,
        max_iterations=max_iters
    )
    assert num_iters == 1
    with pytest.raises(exceptions.MaxRunIterations):
        num_iters = scheduler._update_run_iteration(
            num_iterations=num_iters,
            max_iterations=max_iters
        )


def test__check_default_entry_overwrite(
    scheduler: Scheduler, 
    default_entries: List[entries.ScheduleEntry]
) -> None:
    for entry in default_entries:
        with pytest.raises(exceptions.OverwriteDefaultEntryError):
            scheduler._check_default_entry_overwrite(sched_entry=entry)

