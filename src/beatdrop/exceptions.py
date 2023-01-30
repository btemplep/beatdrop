
class BeatdropError(Exception):
    """Base Beatdrop error.
    """
    pass


class MethodNotImplementedError(BeatdropError):
    """The method called is not implemented.
    """
    pass


class OverwriteDefaultEntryError(BeatdropError):
    """A default entry was cannot be removed.
    """
    pass


class MaxRunIterations(BeatdropError):
    """Exception raised when a scheduler has reached the max iterations.
    """
    pass


class ScheduleEntryNotFound(BeatdropError):
    """The given schedule entry was not found.
    """
    pass


class ScheduleEntryTypeNotRegistered(BeatdropError):
    """The schedule entry type it not registered with the scheduler.
    """
    pass

