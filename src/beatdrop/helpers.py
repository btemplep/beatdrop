
import datetime

def utc_now_naive() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)