
import datetime

from croniter import croniter


def dt_is_naive(v: datetime.datetime) -> datetime.datetime:
    if v.tzinfo is not None:
        raise ValueError("This datetime.datetime object must be naive.")

    return v


def valid_cron_expression(v: str) -> str:
    croniter(v, datetime.datetime.utcnow())

    return v
