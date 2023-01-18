
import datetime
from typing import List

import pytest

from beatdrop import entries
from beatdrop.schedulers import SingletonLockScheduler


def test_creation(default_entries: List[entries.ScheduleEntry]) -> None:
    single_sched =  SingletonLockScheduler(
        max_interval=60,
        default_sched_entries=default_entries,
        lock_timeout=180
    )
    assert type(single_sched.lock_timeout) == datetime.timedelta


def test_short_lock_timeout(default_entries: List[entries.ScheduleEntry]) -> None:
    with pytest.raises(ValueError):
        SingletonLockScheduler(
            max_interval=60,
            default_sched_entries=default_entries,
            lock_timeout=179
        )
