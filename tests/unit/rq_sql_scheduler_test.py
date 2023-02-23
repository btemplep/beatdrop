
from rq import Queue

from beatdrop import RQSQLScheduler, SQLScheduler


def test_rq_sql_scheduler(
    rq_queue: Queue,
    sql_scheduler: SQLScheduler
) -> None:
    sched = RQSQLScheduler(
        max_interval=10,
        lock_timeout=30,
        rq_queue=rq_queue,
        create_engine_kwargs=sql_scheduler.create_engine_kwargs
    )

    assert len(list(sched.list())) == 0

