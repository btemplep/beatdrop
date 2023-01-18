
__all__ = [
    "Scheduler",
    "SingletonLockScheduler",
    "MemScheduler",
    "CeleryScheduler"
]

# Base Schedulers with different features
from beatdrop.schedulers.scheduler import Scheduler
from beatdrop.schedulers.singleton_lock_scheduler import SingletonLockScheduler

# Scheduler implementations without task specifics
from beatdrop.schedulers.mem_scheduler import MemScheduler
try:
    from beatdrop.schedulers.sql_scheduler import SQLScheduler
    __all__.append("SQLScheduler")
except ModuleNotFoundError as error: # pragma: no cover
    pass
try:
    from beatdrop.schedulers.redis_scheduler import RedisScheduler
    __all__.append("RedisScheduler")
except ModuleNotFoundError as error: # pragma: no cover
    pass

# Task backend implementations for send()
try:
    from beatdrop.schedulers.celery_scheduler import CeleryScheduler
    __all__.append("CeleryScheduler")
except ModuleNotFoundError as error: # pragma: no cover
    pass

# Complete Schedulers
try:
    from beatdrop.schedulers.celery_redis_scheduler import CeleryRedisScheduler
    __all__.append("CeleryRedisScheduler")
except ModuleNotFoundError as error: # pragma: no cover
    pass
try:
    from beatdrop.schedulers.celery_sql_scheduler import CelerySQLScheduler
    __all__.append("CelerySQLScheduler")
except ModuleNotFoundError as error: # pragma: no cover
    pass

