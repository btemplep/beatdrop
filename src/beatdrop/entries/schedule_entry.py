

import datetime
from typing import Any, Callable, cast, ClassVar, Dict, List, Optional, Tuple
import jsonpickle

from pydantic import BaseModel, Field

from beatdrop.logger import logger
from beatdrop.exceptions import MethodNotImplementedError


class ScheduleEntry(BaseModel):
    """Base ScheduleEntry.

    All Schedule entries must implement the methods:
    
    * ``due_in`` - returns timedelta until it should be run again.
    * ``sent`` - called by the scheduler to let the entry know its task was sent for execution.

    See their docstrings for more details.

    A basic ``__str__`` method is also included. 
    It's recommended to customize it in subclasses for better logging. 

    Parameters
    ----------
    key : str
        A unique key for the schedule entry.
    enabled : bool
        Enable this entry to be scheduled.
    task : str
        The full python path to the task to run.
    args : Optional[Tuple[Any, ...]]
        Positional arguments to pass the task. 
        These will be serialized/deserialized as JSON. 
        ``jsonpickle`` is used to serialize and deserialize these. 
    kwargs : Optional[Dict[str, Any]]
        Keyword arguments to pass the task. 
        These will be serialized/deserialized as JSON. 
        ``jsonpickle`` is used to serialize and deserialize these. 

    Attributes
    ----------
    _logger : ClassVar
        Logger.
    client_read_only_fields : ClassVar[List[str]] = []
        Client read only list of fields. 
        Enumerates the fields that are not normally saved when a client wants to save the entry.
        This is done because the client manages these fields.
        So they are are updated when the scheduler runs them. 
    """

    _logger: ClassVar = logger
    client_read_only_fields: ClassVar[List[str]] = []

    key: str
    enabled: bool
    task: str
    args: Optional[Tuple[Any, ...]] = Field(default=None)
    kwargs: Optional[Dict[str, Any]] = Field(default=None)

    class Config:

        validate_assignment = True
    

    def due_in(self) -> datetime.timedelta:
        """Returns when the schedule entry should run next.

        Returns
        -------
        datetime.timedelta
            Time left until this entry should be scheduled.
            Zero or negative timedeltas mean it should be run.

        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            Must implement this method in subclass.
        """
        raise MethodNotImplementedError("You must implement the 'due_in' method for a schedule.")

    
    def sent(self) -> None:
        """Called when the entry has been sent for execution.

        This should be used to update any metadata as necessary for the entry.
        Like the last sent time etc.

        Raises
        ------
        beatdrop.exceptions.MethodNotImplementedError
            Must implement this method in subclass.
        """
        raise MethodNotImplementedError("You must implement the 'sent' method for a schedule.")


    def __str__(self) -> str:
        return "{}(key={}, enabled={}, task={}, args={}, kwargs={})".format(
            type(self).__name__,
            self.key,
            self.enabled,
            self.task,
            self.args,
            self.kwargs
        )


    def dict(self, *args, **kwargs) -> dict:
        """Override of the pydantic dict method. 

        Adds the ``__beatdrop_type__`` field for the ``SchedulerEntry`` Class name.

        Returns
        -------
        dict
            Dictionary representation of the model.
        """
        dict_ = super().dict(*args, **kwargs)
        dict_['__beatdrop_type__'] = self.__class__.__name__

        return dict_

    
    def json(self, *args, **kwargs) -> str:
        """Override of the pydantic ``json`` method.

        Adds the ``__beatdrop_type__`` field for the ``SchedulerEntry`` Class name.

        Returns
        -------
        str
            JSON representation of the model.
        """
        data = self.dict(*args, **kwargs)
        data_args = data['args']
        data_kwargs = data['kwargs']
        if data_args is not None:
            data['args'] = jsonpickle.Pickler().flatten(data_args)
        
        if data_kwargs is not None:
            data['kwargs'] = jsonpickle.Pickler().flatten(data_kwargs)

        encoder = cast(Callable[[Any], Any], kwargs.get("encoder") or self.__json_encoder__)
        
        return self.__config__.json_dumps(data, default=encoder, **kwargs.get("dumps_kwargs", {}))
    

    # @validator("task")
    # def task_is_function(v: str):
    #     split_path = v.split(".")
    #     module_path = ".".join(split_path[:-1])
    #     func_name = split_path[-1]
    #     try:
    #         module_ = import_module(module_path)
    #         func_ = getattr(module_, func_name)
    #         if not inspect.isfunction(func_):
    #             raise ValueError()
    #     except Exception as error:
    #         raise ValueError(
    #             "Task '{}' is not a valid path to a python function and could not be imported".format(v)
    #         ) from error 

    #     return v
