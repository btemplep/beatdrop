
import json
from typing import Tuple, Type

import jsonpickle

from beatdrop import ScheduleEntry
from beatdrop.exceptions import ScheduleEntryTypeNotRegistered


class EntryTypeRegistry:
    """Contains the ScheduleEntry types and how to deserialize them from json.
    """

    def __init__(self, sched_entry_types: Tuple[Type[ScheduleEntry]]):
        self.sched_entry_types = sched_entry_types
        self._sched_entry_type_lookup = {entry.__name__: entry for entry in self.sched_entry_types}
        self.jp_unpickler = jsonpickle.Unpickler()


    def dedict_entry(self, sched_entry_dict: dict) -> ScheduleEntry:
        """Rehydrates a model from its dictionary representation.

        Parameters
        ----------
        model_dict : dict
            Dictionary representation of a model based on ``BeatBaseDrop``.

        Returns
        -------
        ScheduleEntry
             Rehydrated model.

        Raises
        ------
        beatdrop.exceptions.ScheduleEntryTypeNotRegistered
            The ScheduleEntry type is not registered with this scheduler.
        """
        entry_type_str = sched_entry_dict['__beatdrop_type__']
        if entry_type_str not in self._sched_entry_type_lookup:
            raise ScheduleEntryTypeNotRegistered(
                "The schedule entry type '{}' is not registered".format(entry_type_str)
            )

        entry_args = sched_entry_dict['args']
        entry_kwargs = sched_entry_dict['kwargs']
        if entry_args is not None:
            sched_entry_dict['args'] = jsonpickle.Unpickler().restore(entry_args)

        if entry_kwargs is not None:
            sched_entry_dict['kwargs'] = jsonpickle.Unpickler().restore(entry_kwargs)
        
        model_ = self._sched_entry_type_lookup[entry_type_str](**sched_entry_dict)

        return model_


    def dejson_entry(self, sched_entry_json: str) -> ScheduleEntry:
        """Rehydrates a schedule entry model from its JSON representation.

        Uses the ``dedict`` method.

        Parameters
        ----------
        model_json : str
            JSON representation of a model based on ``ScheduleEntry``.

        Returns
        -------
        ScheduleEntry
            Rehydrated model.
        """
        return self.dedict_entry(
            json.loads(sched_entry_json)
        )
