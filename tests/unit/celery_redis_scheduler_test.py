
from celery import Celery

from beatdrop import CeleryRedisScheduler, RedisScheduler

def test_celery_redis_scheduler(
    celery_app: Celery, 
    redis_scheduler: RedisScheduler
) -> None:
    sched = CeleryRedisScheduler(
        max_interval=10,
        celery_app=celery_app,
        lock_timeout=30,
        redis_py_kwargs=redis_scheduler.redis_py_kwargs
    )
    assert len(list(sched.list())) == 0
