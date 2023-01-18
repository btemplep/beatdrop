
import datetime
from typing import Callable, List
from unittest.mock import MagicMock

import pytest
from redislite import Redis

from beatdrop.schedulers import RedisScheduler
from beatdrop.entries import IntervalEntry, ScheduleEntry
from beatdrop import entries, exceptions, messages


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
def redis_scheduler2(
    max_interval: datetime.timedelta,
    lock_timeout: datetime.timedelta,
    default_entries: List[ScheduleEntry],
    redis_scheduler: RedisScheduler
) -> RedisScheduler:
    redis_sched =  RedisScheduler(
        max_interval=max_interval,
        default_sched_entries=default_entries,
        lock_timeout=lock_timeout,
        redis_py_kwargs=redis_scheduler.redis_py_kwargs
    )
    redis_sched.send = MagicMock(return_value=None)

    return redis_sched


@pytest.fixture
def redis_scheduler_rdb_entries(
    redis_scheduler: RedisScheduler,
    interval_entry: IntervalEntry
) -> RedisScheduler:
    redis_scheduler.save(interval_entry)
    return redis_scheduler


def test_creation(redis_scheduler: RedisScheduler) -> None:
    assert redis_scheduler._redis_conn.get("hello") == None
    redis_scheduler._redis_conn.set("hello", "there")
    assert redis_scheduler._redis_conn.get("hello") == "there"


def test_save(
    redis_scheduler: RedisScheduler,
    interval_entry: IntervalEntry
) -> None:
    entry_json = redis_scheduler._redis_conn.hget(
        name=redis_scheduler._hash_key,
        key=interval_entry.key
    )
    assert entry_json == None
    redis_scheduler.save(interval_entry)
    entry_json = redis_scheduler._redis_conn.hget(
        name=redis_scheduler._hash_key,
        key=interval_entry.key
    )
    assert entry_json != None
    entry = redis_scheduler._entry_type_registry.dejson_entry(entry_json)
    assert entry == interval_entry
    new_period = datetime.timedelta(seconds=100)
    new_last_sent_at = datetime.datetime.utcnow()
    entry.period = new_period
    entry.last_sent_at = new_last_sent_at
    redis_scheduler.save(entry)
    updated_entry_json = redis_scheduler._redis_conn.hget(
        name=redis_scheduler._hash_key,
        key=entry.key
    )
    updated_entry = redis_scheduler._entry_type_registry.dejson_entry(updated_entry_json)
    assert updated_entry.period == new_period
    assert updated_entry.last_sent_at < new_last_sent_at
    entry.period = new_period
    entry.last_sent_at = new_last_sent_at
    redis_scheduler.save(entry, read_only_attributes=True)
    updated_entry_json = redis_scheduler._redis_conn.hget(
        name=redis_scheduler._hash_key,
        key=entry.key
    )
    updated_entry = redis_scheduler._entry_type_registry.dejson_entry(updated_entry_json)
    assert updated_entry.period == new_period
    assert updated_entry.last_sent_at == new_last_sent_at


def test_list(
    redis_scheduler_rdb_entries: RedisScheduler,
    interval_entry: IntervalEntry,
    default_entries: List[ScheduleEntry]
) -> None:
    entry_list = redis_scheduler_rdb_entries.list()
    assert interval_entry in entry_list
    for entry in default_entries:
        assert entry in entry_list


def test_list_no_redis_entries(
    redis_scheduler: RedisScheduler,
    default_entries: List[ScheduleEntry]
) -> None:
    entry_list = redis_scheduler.list()
    for entry in default_entries:
        assert entry in entry_list


def test_list_paginate(
    redis_scheduler_rdb_entries: RedisScheduler,
    interval_entry: IntervalEntry,
    default_entries: List[ScheduleEntry],
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
    redis_scheduler_rdb_entries.save(some_crontab)
    redis_scheduler_rdb_entries.save(another_entry)
    redis_scheduler_rdb_entries.save(yet_another_entry)
    entry_list = redis_scheduler_rdb_entries.list(page_size=1)
    for entry in default_entries:
        assert entry in entry_list

    assert interval_entry in entry_list
    assert some_crontab in entry_list
    assert another_entry in entry_list
    assert yet_another_entry in entry_list


def test_list_blank_pages(
    redis_scheduler_rdb_entries: RedisScheduler,
    interval_entry: IntervalEntry
) -> None:
    redis_scheduler_rdb_entries._redis_conn.hscan = MagicMock(
        side_effect=[
            (10, {}),
            (10, {}),
            (10, {interval_entry.key: interval_entry.json()}),
            (0, {})
        ]
    )
    results = redis_scheduler_rdb_entries.list(page_size=1)
    assert interval_entry in results


def test_get(
    redis_scheduler_rdb_entries: RedisScheduler,
    interval_entry: IntervalEntry,
    default_entries: List[ScheduleEntry]
) -> None:
    int_entry_get = redis_scheduler_rdb_entries.get(interval_entry.key)
    assert int_entry_get == interval_entry
    default_entry = redis_scheduler_rdb_entries.get(default_entries[0].key)
    assert default_entry == default_entries[0]
    with pytest.raises(exceptions.ScheduleEntryNotFound):
        redis_scheduler_rdb_entries.get("not found here")


def test_delete(
    redis_scheduler_rdb_entries: RedisScheduler,
    interval_entry: IntervalEntry,
    default_entries: List[ScheduleEntry]
) -> None:
    assert interval_entry == redis_scheduler_rdb_entries.get(interval_entry.key)
    redis_scheduler_rdb_entries.delete(interval_entry)
    with pytest.raises(exceptions.ScheduleEntryNotFound):
        redis_scheduler_rdb_entries.get(interval_entry.key)

    assert interval_entry not in redis_scheduler_rdb_entries.list()

    redis_scheduler_rdb_entries.delete(default_entries[0])
    assert default_entries[0] in redis_scheduler_rdb_entries.list()


def test__acquire_lock(
    redis_scheduler: RedisScheduler,
    redis_scheduler2: RedisScheduler
) -> None:
    redis_scheduler._acquire_lock()
    assert redis_scheduler._scheduler_lock.locked() > 0
    assert redis_scheduler2._scheduler_lock.acquire(timeout=.1) == False
    before = datetime.datetime.utcnow()
    redis_scheduler2._acquire_lock()
    after = datetime.datetime.utcnow()
    acquired_after = after - before
    assert acquired_after > redis_scheduler.max_interval
    assert acquired_after < (redis_scheduler.lock_timeout + redis_scheduler.max_interval)


def test__refresh_lock(
    redis_scheduler: RedisScheduler,
    redis_scheduler2: RedisScheduler
) -> None:
    redis_scheduler._acquire_lock()
    assert redis_scheduler._refresh_lock() == True
    assert redis_scheduler2._refresh_lock() == False


def test_run(
    redis_scheduler_rdb_entries: RedisScheduler,
    scheduler_run_tests: Callable
) -> None:
    scheduler_run_tests(redis_scheduler_rdb_entries)


def test_run_lost_lock(
    redis_scheduler_rdb_entries: RedisScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    redis_scheduler_rdb_entries._refresh_lock = MagicMock(side_effect=[False])
    redis_scheduler_rdb_entries.run(max_iterations=2)
    assert caplog.text.count(messages.sched_lock_acquired) == 2


def test_run_critical_error(
    redis_scheduler_rdb_entries: RedisScheduler,
    caplog: pytest.LogCaptureFixture
) -> None:
    redis_scheduler_rdb_entries.send = MagicMock(side_effect=Exception)
    redis_scheduler_rdb_entries.run(max_iterations=3)
    assert "CRITICAL" in caplog.text


def test__cleanup_no_lock(
    redis_scheduler_rdb_entries: RedisScheduler
) -> None:
    # Should not throw an error
    redis_scheduler_rdb_entries._cleanup()


def test__run_once_entry_not_found(
    redis_scheduler: RedisScheduler,
    interval_entry: IntervalEntry
) -> None:
    # Should not throw an error
    redis_scheduler._run_once(sched_entries=[interval_entry])


def test__run_once_disabled_db_entry(
    redis_scheduler_rdb_entries: RedisScheduler,
    interval_entry: IntervalEntry
) -> None:
    interval_entry.enabled = False
    redis_scheduler_rdb_entries.save(interval_entry)
    # Should not throw an error
    redis_scheduler_rdb_entries._run_once()

