

from rq import Queue

from beatdrop import RedisScheduler, RQRedisScheduler


def test_rq_redis_scheduler(
    rq_queue: Queue,
    redis_scheduler: RedisScheduler
) -> None:
    sched = RQRedisScheduler(
        max_interval=10,
        lock_timeout=30,
        rq_queue=rq_queue,
        redis_py_kwargs=redis_scheduler.redis_py_kwargs
    )

    assert len(list(sched.list())) == 0

