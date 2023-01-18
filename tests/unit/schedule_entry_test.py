
import datetime

import pytest

from beatdrop.entries import ScheduleEntry
from beatdrop import exceptions


@pytest.fixture
def sched_entry(
    test_task: str,
    test_args: tuple,
    test_kwargs: dict
)-> ScheduleEntry:
    return ScheduleEntry(
        key="new_entry",
        enabled=True,
        task=test_task,
        args=test_args,
        kwargs=test_kwargs
    )


def test_creation(sched_entry: ScheduleEntry) -> None:
    assert sched_entry.key == "new_entry"


# def test_bad_task() -> None:
#     with pytest.raises(ValueError):
#         ScheduleEntry(
#             key="new_thing",
#             enabled=True,
#             task="task.does.not.exist"
#         )


# def test_not_function_task() -> None:
#     with pytest.raises(ValueError):
#         ScheduleEntry(
#             key="idc",
#             enabled=True,
#             task="beatdrop.entries.schedule_entry.ScheduleEntry"
#         )


def test_not_implemented_methods(sched_entry: ScheduleEntry) -> None:
    with pytest.raises(exceptions.MethodNotImplementedError):
        sched_entry.due_in()

    with pytest.raises(exceptions.MethodNotImplementedError):
        sched_entry.sent()


def test_str(sched_entry: ScheduleEntry) -> None:
    entry_str = sched_entry.__str__()
    assert "ScheduleEntry" in entry_str
    assert "key" in entry_str
    assert "enabled" in entry_str
    assert "task" in entry_str
    assert "args" in entry_str
    assert "kwargs" in entry_str


def test_dict(sched_entry: ScheduleEntry) -> None:
    dict_ = sched_entry.dict()
    assert "__beatdrop_type__" in dict_
    assert dict_['__beatdrop_type__'] == "ScheduleEntry"
    rehydrated_entry = ScheduleEntry(**dict_)
    assert sched_entry == rehydrated_entry


def test_json(sched_entry: ScheduleEntry) -> None:
    json_ = sched_entry.json()
    assert "__beatdrop_type__" in json_

