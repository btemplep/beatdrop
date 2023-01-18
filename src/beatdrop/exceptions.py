
class BeatDropError(Exception):
    pass


class InvalidScheduleEntryType(BeatDropError):
    pass


class MethodNotImplementedError(BeatDropError):
    pass


class OverwriteDefaultEntryError(BeatDropError):
    pass


class SchedulerError(BeatDropError):
    pass


class MaxRunIterations(BeatDropError):
    pass


class ScheduleEntryNotFound(BeatDropError):
    pass


class ScheduleEntryTypeNotRegistered(BeatDropError):
    pass

