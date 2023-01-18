
import datetime
from pathlib import Path
from typing import Callable, List
from unittest.mock import MagicMock

import pytest

from beatdrop import entries, messages, exceptions
from beatdrop.entries import IntervalEntry, ScheduleEntry
from beatdrop.schedulers.sql_scheduler import SQLScheduler, SQLScheduleEntry, SQLSchedulerLock


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


@pytest.fixture
def sql_scheduler2(
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

    return sql_sched


@pytest.fixture
def sql_scheduler_w_db_entry(
    sql_scheduler: SQLScheduler,
    interval_entry: IntervalEntry
) -> SQLScheduler:
    sql_scheduler.save(interval_entry)
    
    return sql_scheduler


def test_creation(
    sql_scheduler: SQLScheduler,
    sql_scheduler2: SQLScheduler
) -> None:
    assert "url" in sql_scheduler.create_engine_kwargs
    assert "url" in sql_scheduler2.create_engine_kwargs


def test_creation_no_url(
    max_interval: datetime.timedelta,
    lock_timeout: datetime.timedelta,
    default_entries: List[entries.ScheduleEntry]
) -> None:
    with pytest.raises(ValueError):
        SQLScheduler(
            max_interval=max_interval,
            default_sched_entries=default_entries,
            lock_timeout=lock_timeout,
            create_engine_kwargs={}
        )


def test_save_create(
    sql_scheduler: SQLScheduler,
    interval_entry: entries.ScheduleEntry
) -> None:
    with sql_scheduler._Session() as sess:
        db_entries = sess.query(SQLScheduleEntry).all()

    assert len(db_entries) == 0
    sql_scheduler.save(interval_entry)
    with sql_scheduler._Session() as sess:
        db_entries = sess.query(SQLScheduleEntry).all()

    assert len(db_entries) == 1
    rehydrated_entry = sql_scheduler._entry_type_registry.dejson_entry(db_entries[0].json_)
    assert interval_entry == rehydrated_entry


def test_save_update_ro_attributes_false(
    sql_scheduler: SQLScheduler,
    interval_entry: entries.IntervalEntry
) -> None:
    original_last_sent_at = interval_entry.last_sent_at
    sql_scheduler.save(interval_entry)
    new_period = datetime.timedelta(seconds=interval_entry.period.total_seconds() + 1)
    interval_entry.period = new_period
    new_last_sent_at = datetime.datetime.utcnow()
    interval_entry.last_sent_at = new_last_sent_at
    sql_scheduler.save(interval_entry)
    with sql_scheduler._Session() as sess:
        db_entries = sess.query(SQLScheduleEntry).all()

    assert len(db_entries) == 1
    rehydrated_entry: IntervalEntry = sql_scheduler._entry_type_registry.dejson_entry(db_entries[0].json_)
    assert rehydrated_entry.last_sent_at == original_last_sent_at
    assert rehydrated_entry.last_sent_at != new_last_sent_at
    assert rehydrated_entry.last_sent_at == interval_entry.last_sent_at
    assert rehydrated_entry.period == new_period 


def test_save_update_ro_attributes_true(
    sql_scheduler: SQLScheduler,
    interval_entry: entries.IntervalEntry
) -> None:
    original_last_sent_at = interval_entry.last_sent_at
    sql_scheduler.save(interval_entry)
    new_period = datetime.timedelta(seconds=interval_entry.period.total_seconds() + 1)
    interval_entry.period = new_period
    new_last_sent_at = datetime.datetime.utcnow()
    interval_entry.last_sent_at = new_last_sent_at
    sql_scheduler.save(interval_entry, read_only_attributes=True)
    with sql_scheduler._Session() as sess:
        db_entries = sess.query(SQLScheduleEntry).all()

    assert len(db_entries) == 1
    rehydrated_entry: IntervalEntry = sql_scheduler._entry_type_registry.dejson_entry(db_entries[0].json_)
    assert rehydrated_entry.last_sent_at != original_last_sent_at
    assert rehydrated_entry.last_sent_at == new_last_sent_at
    assert rehydrated_entry.last_sent_at == interval_entry.last_sent_at
    assert rehydrated_entry.period == new_period 


def test_list(
    sql_scheduler_w_db_entry: SQLScheduler,
    interval_entry: entries.IntervalEntry,
    default_entries: List[entries.ScheduleEntry]
) -> None:
    entry_list = sql_scheduler_w_db_entry.list()
    for entry in default_entries:
        assert entry in entry_list

    assert interval_entry in entry_list


def test_list_paginate(
    sql_scheduler_w_db_entry: SQLScheduler,
    interval_entry: entries.IntervalEntry,
    default_entries: List[entries.ScheduleEntry],
    test_task: str
) -> None:
    some_crontab = entries.CrontabEntry(
        key="some_other_crontab_entry_here",
        enabled=True,
        task=test_task,
        args=None,
        kwargs=None,
        cron_expression="*/1 * * * *",
    )
    another_entry = entries.CrontabEntry(
        key="another_crontab_entry",
        enabled=True,
        task=test_task,
        args=None,
        kwargs=None,
        cron_expression="*/1 * * * *",
    )
    yet_another_entry = entries.CrontabEntry(
        key="yet_another_crontab_entry",
        enabled=True,
        task=test_task,
        args=None,
        kwargs=None,
        cron_expression="*/1 * * * *",
    )
    sql_scheduler_w_db_entry.save(some_crontab)
    sql_scheduler_w_db_entry.save(another_entry)
    sql_scheduler_w_db_entry.save(yet_another_entry)
    entry_list = sql_scheduler_w_db_entry.list(page_size=1)
    for entry in default_entries:
        assert entry in entry_list

    assert interval_entry in entry_list
    assert some_crontab in entry_list
    assert another_entry in entry_list
    assert yet_another_entry in entry_list


def test_get(
    sql_scheduler_w_db_entry: SQLScheduler,
    interval_entry: entries.IntervalEntry,
    default_entries: List[entries.ScheduleEntry]
) -> None:
    default_entry = sql_scheduler_w_db_entry.get(default_entries[0].key)
    assert default_entry == default_entries[0]
    db_entry = sql_scheduler_w_db_entry.get(interval_entry.key)
    assert db_entry == interval_entry
    with pytest.raises(exceptions.ScheduleEntryNotFound):
        sql_scheduler_w_db_entry.get("not found here")


def test_delete(
    sql_scheduler_w_db_entry: SQLScheduler,
    interval_entry: entries.IntervalEntry
) -> None:
    assert interval_entry in sql_scheduler_w_db_entry.list()
    sql_scheduler_w_db_entry.delete(interval_entry)
    assert interval_entry not in sql_scheduler_w_db_entry.list()


def test__acquire_lock(
    lock_timeout: datetime.timedelta,
    sql_scheduler: SQLScheduler,
    sql_scheduler2: SQLScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    before = datetime.datetime.utcnow()
    sql_scheduler._acquire_lock()
    assert messages.sched_lock_acquired in caplog.text
    with sql_scheduler._Session() as session:
        assert len(session.query(SQLSchedulerLock).all()) == 1

    caplog.clear()
    sql_scheduler2._acquire_lock()
    assert messages.sched_lock_acquired in caplog.text
    after = datetime.datetime.utcnow()
    assert after - before > lock_timeout


def test__acquire_lock_dead_scheduler(
    sql_scheduler: SQLScheduler,
    sql_scheduler2: SQLScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    sql_scheduler._acquire_lock()
    assert messages.sched_lock_acquired in caplog.text
    before = datetime.datetime.utcnow()
    caplog.clear()
    sql_scheduler2._acquire_lock()
    assert messages.sched_lock_acquired in caplog.text
    after = datetime.datetime.utcnow()
    second_acquire_delta = after - before
    assert second_acquire_delta > (sql_scheduler.lock_timeout)
    assert second_acquire_delta < (sql_scheduler.lock_timeout + sql_scheduler.max_interval)


def test__refresh_lock(sql_scheduler: SQLScheduler) -> None:
    sql_scheduler._acquire_lock()
    with sql_scheduler._Session() as sess:
        lock_entry: SQLSchedulerLock = sess.query(SQLSchedulerLock).one()
        acquired_at = lock_entry.last_refreshed_at

    is_refreshed = sql_scheduler._refresh_lock()
    assert is_refreshed == True
    with sql_scheduler._Session() as sess:
        lock_entry: SQLSchedulerLock = sess.query(SQLSchedulerLock).one()
        refreshed_at = lock_entry.last_refreshed_at

    assert refreshed_at > acquired_at


def test__refresh_lock_fail(
    sql_scheduler: SQLScheduler,
    sql_scheduler2: SQLScheduler,
) -> None:
    sql_scheduler._acquire_lock()
    with sql_scheduler._Session() as sess:
        lock_entry: SQLSchedulerLock = sess.query(SQLSchedulerLock).one()
        acquired_at = lock_entry.last_refreshed_at

    is_refreshed = sql_scheduler2._refresh_lock()
    assert is_refreshed == False
    with sql_scheduler._Session() as sess:
        lock_entry: SQLSchedulerLock = sess.query(SQLSchedulerLock).one()
        refreshed_at = lock_entry.last_refreshed_at

    assert refreshed_at == acquired_at


def test_run(
    sql_scheduler_w_db_entry: SQLScheduler,
    scheduler_run_tests: Callable
) -> None:
    scheduler_run_tests(sql_scheduler_w_db_entry)


def test_run_lost_lock(
    sql_scheduler_w_db_entry: SQLScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    sql_scheduler_w_db_entry._refresh_lock = MagicMock(side_effect=[False])
    sql_scheduler_w_db_entry.run(max_iterations=2)
    assert caplog.text.count(messages.sched_lock_acquired) == 2


def test_run_critical_error(
    sql_scheduler_w_db_entry: SQLScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    sql_scheduler_w_db_entry.send = MagicMock(side_effect=Exception)
    sql_scheduler_w_db_entry.run(max_iterations=3)
    assert "CRITICAL" in caplog.text


def test__cleanup_no_lock(
    sql_scheduler_w_db_entry: SQLScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    sql_scheduler_w_db_entry._acquire_lock()
    sql_scheduler_w_db_entry._lock_last_refreshed_at = datetime.datetime.utcnow()
    # Should not throw an error
    sql_scheduler_w_db_entry._cleanup()


def test__run_once_entry_not_found(
    sql_scheduler: SQLScheduler,
    interval_entry: IntervalEntry
) -> None:
    entries = [interval_entry] + sql_scheduler.default_sched_entries
    # Should not throw an error
    sql_scheduler._run_once(sched_entries=entries)


def test__run_once_disabled_db_entry(
    sql_scheduler: SQLScheduler,
    interval_entry: IntervalEntry
) -> None:
    interval_entry.enabled = False
    sql_scheduler.save(interval_entry)
    # Should not throw an error
    sql_scheduler._run_once()

