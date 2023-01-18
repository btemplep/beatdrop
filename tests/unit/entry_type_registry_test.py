
from typing import List

import pytest

from beatdrop.entry_type_registry import EntryTypeRegistry
from beatdrop import ScheduleEntry, default_sched_entry_types, exceptions


@pytest.fixture(scope="function")
def entry_type_registry() -> EntryTypeRegistry:
    return EntryTypeRegistry(sched_entry_types=default_sched_entry_types)


def test_dejson(
    default_entries: List[ScheduleEntry],
    entry_type_registry: EntryTypeRegistry
) -> None:
    for entry in default_entries:
        json_ = entry.json()
        rehydrated = entry_type_registry.dejson_entry(json_)
        assert entry == rehydrated


def test_dejson_unknown_entry_type(entry_type_registry: EntryTypeRegistry, test_task: str) -> None:
    class UnknownEntry(ScheduleEntry):
        some_int: int

    some_entry = UnknownEntry(
        key="unknown_thing",
        enabled=True,
        task=test_task,
        some_int=12
    )
    json_ = some_entry.json()
    with pytest.raises(exceptions.ScheduleEntryTypeNotRegistered):
        entry_type_registry.dejson_entry(json_)

