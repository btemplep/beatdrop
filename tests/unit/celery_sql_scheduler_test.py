
from celery import Celery

from beatdrop import CelerySQLScheduler, SQLScheduler

def test_celery_sql_scheduler(
    celery_app: Celery, 
    sql_scheduler: SQLScheduler
) -> None:
    sched = CelerySQLScheduler(
        max_interval=10,
        celery_app=celery_app,
        lock_timeout=30,
        create_engine_kwargs=sql_scheduler.create_engine_kwargs
    )

    assert len(list(sched.list())) == 0

