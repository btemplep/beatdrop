
import copy
from datetime import timedelta
import json
import time
from typing import Any, Dict, List, Optional

import pottery
from pydantic.dataclasses import dataclass
from redis import Redis

from beatdrop import art
from beatdrop import messages
from beatdrop.schedulers.singleton_lock_scheduler import SingletonLockScheduler
from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop.entry_type_registry import EntryTypeRegistry
from beatdrop import exceptions


class RedisScheduleEntryList: 
    """Iterator for RedisScheduler entries.

    Parameters
    ----------
    page_size : int
        Redis suggested minimum page size.
    default_sched_entries : List[ScheduleEntry]
        Default schedule entries that will be iterated over first.
    redis_conn : Redis
        Redis connection object.
    hash_key : str
        Redis key of the hash that stores schedule entries.
    entry_type_registry : EntryTypeRegistry
        Entry type registry for deserializing JSON models from redis.
    """

    def __init__(
        self, 
        page_size: int, 
        default_sched_entries: List[ScheduleEntry], 
        redis_conn: Redis,
        hash_key: str,
        entry_type_registry: EntryTypeRegistry
    ):
        self._redis_conn = redis_conn
        self.page_size = page_size
        self._default_sched_entries = default_sched_entries
        self._default_entries_iter = iter(self._default_sched_entries)
        self._hash_key = hash_key
        self._entry_type_registry = entry_type_registry
        self._redis_page_iter = None
        self._cursor = None


    def __iter__(self):
        self._default_entries_iter = iter(self._default_sched_entries)
        self._iterated_default_entries = False
        self._redis_page_iter = None
        self._cursor = None

        return self


    def __next__(self):
        if self._iterated_default_entries == False:
            try:
                return next(self._default_entries_iter)
            except StopIteration:
                self._iterated_default_entries = True
        
        if self._iterated_default_entries == True:
            if self._redis_page_iter is None:
                self._cursor, results = self._redis_conn.hscan(
                    name=self._hash_key,
                    cursor=0,
                    count=self.page_size
                )
                self._redis_page_iter = iter(results.values())

            try:
                return self._entry_type_registry.dejson_entry(
                        sched_entry_json=next(self._redis_page_iter)
                    )
            except StopIteration:
                return self._get_next_page_item()
    
    
    def _get_next_page_item(self) -> ScheduleEntry:
        """Update the page data and return the next schedule entry.

        This function is recursive because there is a chance that redis will
        return no results, but the cursor says that there are still results.
        https://redis.io/commands/scan/

        Returns
        -------
        ScheduleEntry
            Next schedule entry to return.

        Raises
        ------
        StopIteration
            When there are no more entries to return.
        """
        if self._cursor == 0:
            raise StopIteration
        
        self._cursor, results = self._redis_conn.hscan(
            name=self._hash_key,
            cursor=self._cursor,
            count=self.page_size
        )
        self._redis_page_iter = iter(results.values())
        try:
            return self._entry_type_registry.dejson_entry(
                sched_entry_json=next(self._redis_page_iter)
            )
        except StopIteration:
            return self._get_next_page_item()


@dataclass
class RedisScheduler(SingletonLockScheduler):
    """Hold schedule entries in a Redis. 

    Uses Redis to store schedule entries and scheduler state.
    It is safe to run multiple ``RedisScheduler`` s simultaneously, 
    as well as have many that are used as clients to read/write entries.

    This scheduler does not implement the ``send`` method.
    This must be implemented before it can actually send tasks
    to the specified backend.

    Parameters
    ----------
    max_interval : datetime.timedelta
        The maximum interval that the scheduler should sleep before waking up to check for due tasks.
    sched_entry_types : Tuple[Type[ScheduleEntry]], default : (CrontabEntry, CrontabTZEntry, EventEntry, IntervalEntry)
        A list of valid schedule entry types for this scheduler.
        These are only stored in the scheduler, not externally.
    default_sched_entries : List[ScheduleEntry], default : []
        Default list of schedule entries.  
        In general these entries are not held in non-volatile storage 
        so any metadata they hold will be lost if the scheduler fails.
        These entries are static.  The keys cannot be overwritten or deleted.
    lock_timeout : datetime.timedelta
        The time a scheduler does not refresh the scheduler lock before it is considered dead. 
        Should be at least 3 times the ``max_interval``.
    redis_py_kwargs : Dict[str, Any]
        redis-py's ``redis.Redis()`` key word arguments. Some of the client configuration items may be overwritten.
        https://redis-py.readthedocs.io/en/stable/connections.html#generic-client
    """

    redis_py_kwargs: Dict[str, Any]


    def __post_init_post_parse__(self) -> None:
        super().__post_init_post_parse__()
        self.redis_py_kwargs['decode_responses'] = True
        self._zero_delta = timedelta(seconds=0)
        self._scheduler_lock_key = "beatdrop_scheduler_lock"
        self._entry_lock_prefix = "beatdrop_entry_lock:"
        self._hash_key = "beatdrop_entries"
        self._redis_conn = Redis(
            **self.redis_py_kwargs
        )
        self._redis_masters = {self._redis_conn}
        self._scheduler_lock = pottery.Redlock(
            key=self._scheduler_lock_key, 
            masters=self._redis_masters, 
            auto_release_time=self.lock_timeout.total_seconds(),
            num_extensions=3 # this can't be unlimited!
        )


    def _acquire_lock(self) -> None:
        """Acquire the scheduler lock.

        Will wait indefinitely until the scheduler lock is acquired.
        This method should only be called by the scheduler ``run`` method.
        **Never by a client.**
        """
        self._logger.debug(messages.sched_lock_acquiring)
        while True:
            acquired = self._scheduler_lock.acquire(timeout=1)
            if acquired:
                self._logger.info(messages.sched_lock_acquired)

                return

            self._logger.debug(
                messages.sched_lock_wait_template.format(
                    self.max_interval.total_seconds()
                )
            )
            time.sleep(self.max_interval.total_seconds())
 

    def run(self, max_iterations: int = None) -> None:
        """Run the scheduler.

        Parameters
        ----------
        max_iterations: int 
            default : None

            The maximum number of iterations to run the scheduler.
            None is unlimited.
        """
        try:
            self._logger.info(art.logo)
            self._acquire_lock()
            self._logger.info(messages.scheduler_starting)
            num_iterations = 0
            while True:
                # pull all keys that are schedule entries
                self._logger.debug(messages.scheduler_pulling_entries)
                sleep_time = self._run_once()
                num_iterations = self._update_run_iteration(
                    num_iterations=num_iterations, 
                    max_iterations=max_iterations
                )
                lock_refreshed = self._refresh_lock()
                if lock_refreshed:
                    self._logger.debug(
                        messages.scheduler_sleep_template.format(sleep_time.total_seconds())
                    )
                    time.sleep(sleep_time.total_seconds())
                else:
                    self._acquire_lock()

        except (exceptions.MaxRunIterations, KeyboardInterrupt):
            self._logger.debug(messages.scheduler_shutting_down)
        
        except Exception as error:
            self._logger.critical("{}: {}".format(type(error).__name__, error))
        
        finally:
            self._cleanup()

    def _cleanup(self):
        self._logger.debug(messages.sched_lock_releasing)
        try:
            self._scheduler_lock.release()
            self._logger.info(messages.sched_lock_released)
        except pottery.exceptions.ReleaseUnlockedLock:
            pass

        self._logger.info(messages.scheduler_shut_down)


    def _run_once(self, sched_entries: Optional[List[ScheduleEntry]] = None) -> timedelta:
        """Run an iteration of the scheduler with given context.

        Parameters
        ----------
        sched_entry_keys : Optional[Dict[str, None]], optional
            Scheduler entry keys, by default None

        Returns
        -------
        timedelta
            Sleep time until the scheduler should wake up and run again.
        """
        sleep_time = self.max_interval
        if sched_entries is None:
            sched_entries = self.list()
        
        for entry in sched_entries:
            if entry.key in self._default_sched_entry_lookup:
                sched_entry = self._default_sched_entry_lookup[entry.key]
                if sched_entry.enabled == True:
                    due_in = sched_entry.due_in() 
                    if due_in <= self._zero_delta:
                        sched_entry.sent()
                        self.send(sched_entry)
                    elif due_in < sleep_time:
                        sleep_time = due_in
            else:
                entry_lock = pottery.Redlock(
                    key=self._entry_lock_prefix + entry.key,
                    masters=self._redis_masters
                )
                with entry_lock:
                    entry_json = self._redis_conn.hget(
                        name=self._hash_key,
                        key=entry.key
                    )
                    if entry_json is None:
                        continue

                    sched_entry = self._entry_type_registry.dejson_entry(entry_json)
                    if sched_entry.enabled == False:
                        continue

                    due_in = sched_entry.due_in()
                    entry_is_due = due_in <= self._zero_delta
                    if entry_is_due:
                        self._logger.debug(messages.sched_entry_due_template.format(sched_entry))
                        sched_entry.sent()
                        self._redis_conn.hset(
                            name=self._hash_key,
                            key=entry.key,
                            value=sched_entry.json()
                        )

                # once the lock is free, actually send the entry
                # Done here to avoid lock contention, if this takes a sizable amount of time or there are any errors.
                if entry_is_due:
                    self.send(sched_entry)

                if due_in < sleep_time:
                    sleep_time = due_in
                    
        return sleep_time

    
    def _refresh_lock(self) -> bool:
        """Refresh the scheduler lock.

        This method should only be called by the scheduler ``run`` method.
        **Never by a client.**

        Returns
        -------
        bool
            ``True`` if the lock was successfully refreshed, or else ``False``.
        """
        self._logger.debug(messages.sched_lock_refreshing)
        # Because pottery does not support unlimited extensions on the lock we set this to 0
        # https://github.com/brainix/pottery/pull/693
        self._scheduler_lock._extension_num = 0
        try:
            self._scheduler_lock.extend()
            self._logger.debug(messages.sched_lock_refreshed)
        except pottery.exceptions.ExtendUnlockedLock:
            self._logger.error(messages.sched_lock_lost)
            return False

        return True


    def save(
        self, 
        sched_entry: ScheduleEntry,
        read_only_attributes: bool = False    
    ) -> None:
        """Save a new, or update an existing schedule entry in redis.

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Schedule entry to create or update.
        read_only_attributes : bool, optional
            If true, read only attributes are also saved to the DB.
            Clients should almost always leave this false, by default False
        """
        self._check_default_entry_overwrite(sched_entry=sched_entry)
        entry_lock = pottery.Redlock(
            key=self._entry_lock_prefix + sched_entry.key,
            masters=self._redis_masters
        )
        with entry_lock:
            entry_json = self._redis_conn.hget(
                name=self._hash_key,
                key=sched_entry.key
            )
            if entry_json is None:
                self._redis_conn.hset(
                    name=self._hash_key, 
                    key=sched_entry.key,
                    value=sched_entry.json()
                )
            else:
                if read_only_attributes == False:
                    entry_dict = json.loads(entry_json)
                    for ro_field in sched_entry.client_read_only_fields:
                        setattr(sched_entry, ro_field, entry_dict[ro_field])

                self._redis_conn.hset(
                    name=self._hash_key,
                    key=sched_entry.key, 
                    value=sched_entry.json()
                )


    def list(self, page_size: int = 500) -> RedisScheduleEntryList:
        """List schedule entries.

        Parameters
        ----------
        page_size : int, optional
            Redis suggested minimum page size, by default 500

        Returns
        -------
        RedisScheduleEntryList
            Iterator of all schedule entries.  Automatically paginated redis results.
        """
        return RedisScheduleEntryList(
            page_size=page_size,
            default_sched_entries=self.default_sched_entries,
            redis_conn=self._redis_conn,
            hash_key=self._hash_key,
            entry_type_registry=self._entry_type_registry
        )


    def get(self, key: str) -> ScheduleEntry:
        """Retrieve a schedule entry by its key.

        Parameters
        ----------
        key : str
            The schedule entry key.

        Returns
        -------
        ScheduleEntry
            The schedule entry with the matching key.

        Raises
        ------
        beatdrop.exceptions.ScheduleEntryNotFound
            The schedule entry could not be found.
        """
        if key in self._default_sched_entry_lookup:
            return self._default_sched_entry_lookup[key]
        
        entry_json = self._redis_conn.hget(
            name=self._hash_key,
            key=key,
        )
        if entry_json is None:
            raise exceptions.ScheduleEntryNotFound(messages.sched_entry_not_found_template.format(key))
            
        return self._entry_type_registry.dejson_entry(entry_json)


    def delete(self, sched_entry: ScheduleEntry) -> None:
        """Delete a schedule entry from the scheduler.

        This does not delete default entries.

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Scheduler entry to delete from the scheduler.

        """
        self._redis_conn.hdel(
            self._hash_key,
            sched_entry.key
        )