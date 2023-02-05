
import copy
from datetime import datetime, timedelta
import json
import time
from typing import Any, Dict, List, Optional

from pydantic import validator
from pydantic.dataclasses import dataclass
import sqlalchemy
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, DateTime, Integer, String

from beatdrop import art, messages
from beatdrop.entry_type_registry import EntryTypeRegistry
from beatdrop.schedulers.singleton_lock_scheduler import SingletonLockScheduler
from beatdrop.entries.schedule_entry import ScheduleEntry
from beatdrop import exceptions


SQLBase = declarative_base()

class SQLScheduleEntry(SQLBase):
    """Table to hold schedule entries in an SQL DB.
    
    - ``key_`` holds the scheduler entry key. 
    - ``json_`` holds the serialized JSON for the scheduler entry.
    """
    
    __tablename__ = "beatdrop_entries"

    key_id = Column(Integer, primary_key=True, autoincrement=True)
    key_ = Column(String, unique=True)
    json_ = Column(String)


class SQLSchedulerLock(SQLBase):
    """Scheduler lock table.

    In order to strive for only one scheduler actively sending tasks,
    there is a separate table that will help to manage this.
    This table only has one column ``last_refreshed_at`` which is a 
    datetime in UTC.  

    This table should only ever have 0 or 1 row. 

    The scheduler lock is managed by using db ``FOR UPDATE`` queries and the datetime value of ``last_refreshed_at``.
    """

    __tablename__ = "beatdrop_scheduler_lock"

    last_refreshed_at = Column(DateTime, primary_key=True)


class SQLScheduleEntryList: 
    """Iterator for SQLSchedule entries.

    Parameters
    ----------
    page_size : int
        Page size for DB pagination
    default_sched_entries : List[ScheduleEntry]
        Default schedule entries that will be iterated over first.
    session_maker : sessionmaker
        SQLAlchemy Session maker to query DB.
    entry_type_registry : EntryTypeRegistry
        Entry type registry for deserializing JSON models from the DB.
    """

    def __init__(
        self, 
        page_size: int, 
        default_sched_entries: List[ScheduleEntry], 
        session_maker: sessionmaker,
        entry_type_registry: EntryTypeRegistry
    ):
        self._Session = session_maker
        self.page_size = page_size
        self._default_sched_entries = default_sched_entries
        self._default_entries_iter = iter(self._default_sched_entries)
        self._entry_type_registry = entry_type_registry
        self._db_page_iter = None
        self._next_page = None


    def __iter__(self):
        self._default_entries_iter = iter(self._default_sched_entries)
        self._iterated_default_entries = False
        self._db_page_iter = None
        self._next_page = None

        return self


    def __next__(self):
        if self._iterated_default_entries == False:
            try:
                return next(self._default_entries_iter)
            except StopIteration:
                self._iterated_default_entries = True
        
        if self._iterated_default_entries == True:
            # start listing from db by page size
            if self._db_page_iter is None:
                with self._Session() as session:
                    results = session.query(SQLScheduleEntry).limit(self.page_size + 1).all()

                if len(results) > self.page_size:
                    results.pop(len(results) - 1)
                    self._next_page = results[-1].key_id
                else:
                    self._next_page = None

                self._db_page_iter = iter(results)
                
            try:
                return self._entry_type_registry.dejson_entry(
                        sched_entry_json=next(self._db_page_iter).json_
                    )
            except StopIteration:
                if self._next_page is None:
                    raise StopIteration

                self._get_next_page()
                return self._entry_type_registry.dejson_entry(
                    sched_entry_json=next(self._db_page_iter).json_
                )


    def _get_next_page(self):
        with self._Session() as session:
            results = session.query(SQLScheduleEntry).filter(
                SQLScheduleEntry.key_id > self._next_page
            ).limit(
                self.page_size + 1
            ).all()
        
        if len(results) > self.page_size:
            results.pop(len(results) - 1)
            self._next_page = results[-1].key_id
        else:
            self._next_page = None

        self._db_page_iter = iter(results)



@dataclass
class SQLScheduler(SingletonLockScheduler):
    """Hold schedule entries in an SQL database. 

    Uses an SQL database to store schedule entries and scheduler state.
    It is safe to run multiple ``SQLScheduler`` s simultaneously, 
    as well as have many that are purely used as clients to read/write entries.

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
    lock_timeout: datetime.timedelta
        The time a scheduler does not refresh the scheduler lock before it is considered dead. 
        Should be at least 3 times the ``max_interval``.
    create_engine_kwargs: dict
        Keyword arguments to pass to ``sqlalchemy.create_engine``.
        See SQLAlchemy docs for more info. 
        https://docs.sqlalchemy.org/en/14/core/engines.html#sqlalchemy.create_engine
    """

    create_engine_kwargs: Dict[str, Any]


    def __post_init_post_parse__(self) -> None:
        super().__post_init_post_parse__()
        self._lock_last_refreshed_at = None
        self._engine = sqlalchemy.create_engine(**self.create_engine_kwargs)
        self._Session = sessionmaker(bind=self._engine)
        self._zero_delta = timedelta(seconds=0)

    
    def _acquire_lock(self) -> None:
        """Acquire the scheduler lock.

        Will wait indefinitely until the scheduler lock is acquired.
        This method should only be called by the scheduler ``run`` method.
        **Never by a client.**
        """
        self._logger.info(messages.sched_lock_acquiring)
        while True:
            utc_now = datetime.utcnow()
            with self._Session() as session:
                # get table lock
                db_lock_result = session.query(SQLSchedulerLock).populate_existing().with_for_update().all()
                if len(db_lock_result) < 1:
                    self._logger.debug(messages.sched_lock_creating)
                    session.add(
                        SQLSchedulerLock(
                            last_refreshed_at=utc_now
                        )
                    )
                    # release table lock
                    session.commit()
                    self._lock_last_refreshed_at = utc_now
                    self._logger.info(messages.sched_lock_acquired)
                    
                    return 

                elif ( # check if lock is expired 
                    (utc_now - db_lock_result[0].last_refreshed_at) > self.lock_timeout
                ): 
                    self._logger.debug(messages.sched_lock_expired)
                    db_lock = db_lock_result[0]
                    db_lock.last_refreshed_at = utc_now
                    # release table lock
                    session.commit()
                    self._lock_last_refreshed_at = utc_now
                    self._logger.info(messages.sched_lock_acquired)
                    
                    return 

                else:
                    self._logger.debug(messages.sched_lock_unavailable)
                    # release table lock
                    session.rollback()

            self._logger.debug(messages.sched_lock_wait_template.format(self.max_interval.total_seconds()))
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
            sleep_time = self.max_interval
            num_iterations = 0
            while True:
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


    def _run_once(
        self,
        sched_entries: Optional[List[ScheduleEntry]] = None
    ) -> timedelta:
        """Run an iteration of the scheduler with given context.

        Parameters
        ----------
        sched_entries: Optional[List[ScheduleEntry]]
            Schedule entry list.  If None, will pull it from default and DB.

        Returns
        -------
        timedelta
            Sleep time until the scheduler should wake up and run again.
        """
        sleep_time = self.max_interval
        if sched_entries is None:
            sched_entries = self.list()
        with self._Session() as session:
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
                    # get column lock
                    db_entry = session.query(SQLScheduleEntry).populate_existing().with_for_update().filter(
                        SQLScheduleEntry.key_ == entry.key
                    ).one_or_none()
                    if db_entry is None:
                        # release  column lock because the entry doesn't exist
                        session.rollback()
                        continue
                    
                    sched_entry = self._entry_type_registry.dejson_entry(db_entry.json_) 
                    if sched_entry.enabled == False:
                        # release column lock because it's not enabled.
                        session.rollback()
                        continue
                    
                    due_in = sched_entry.due_in()
                    entry_is_due = due_in <= self._zero_delta
                    if entry_is_due:
                        self._logger.debug(messages.sched_entry_due_template.format(sched_entry))
                        sched_entry.sent()
                        db_entry.json_ = sched_entry.json()
                        # Release column lock
                        session.commit()
                    else:
                        # Release column lock
                        session.rollback()

                    # once the lock is free, actually send the entry
                    # Done here to avoid lock contention, if this takes a sizable amount of time or there are any errors.
                    if entry_is_due:
                        self.send(sched_entry)

                    if due_in < sleep_time:
                        sleep_time = due_in
                    
            return sleep_time
                    

    def _cleanup(self) -> None:
        with self._Session() as session:
            db_lock_result = session.query(SQLSchedulerLock).populate_existing().with_for_update().all()
            if db_lock_result[0].last_refreshed_at == self._lock_last_refreshed_at:
                self._logger.debug(messages.sched_lock_releasing)
                session.delete(db_lock_result[0])
                # Release scheduler lock
                session.commit()
                self._lock_last_refreshed_at = None
                self._logger.info(messages.sched_lock_released)
            else:
                session.rollback()
        
        self._logger.info(messages.scheduler_shut_down)


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
        utc_now = datetime.utcnow()
        with self._Session() as session:
            # get table lock
            db_lock_result = session.query(SQLSchedulerLock).populate_existing().with_for_update().all()
            if db_lock_result[0].last_refreshed_at == self._lock_last_refreshed_at:
                db_lock_result[0].last_refreshed_at = utc_now
                # release table lock
                session.commit()
                self._logger.debug(messages.sched_lock_refreshed)
                self._lock_last_refreshed_at = utc_now
                
                return True

            else:
                self._logger.error(messages.sched_lock_lost)
                # release table lock
                session.rollback()
                self._lock_last_refreshed_at = None
                
                return False


    def save(
        self, 
        sched_entry: ScheduleEntry,
        read_only_attributes: bool = False    
    ) -> None:
        """Save a new, or update an existing schedule entry in the DB.

        If ``read_only_attributes`` is set to ``False``, 
        ``sched_entry``'s read only attributes will be set to what's in the DB.

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Schedule entry to create or update.
        read_only_attributes : bool, optional
            If true, read only attributes are also saved to the DB.
            Clients should almost always leave this false, by default False
        """
        self._check_default_entry_overwrite(sched_entry=sched_entry)
        with self._Session() as session:
            # Get lock for the column
            db_entry = session.query(
                SQLScheduleEntry
            ).populate_existing().with_for_update().filter(
                SQLScheduleEntry.key_ == sched_entry.key
            ).one_or_none()
            if db_entry is None: # If it doesn't exit create the entry
                session.add(
                    SQLScheduleEntry(
                        key_=sched_entry.key,
                        json_=sched_entry.json() 
                    )
                )
            else: # Update it
                if not read_only_attributes:
                    # If we aren't setting the read only attributes get them from the db first
                    entry_dict = json.loads(db_entry.json_)
                    for ro_field in sched_entry.client_read_only_fields:
                        setattr(sched_entry, ro_field, entry_dict[ro_field])
                    
                # Update the whole entry
                db_entry.json_ = sched_entry.json()                

            # release lock
            session.commit()


    def list(self, page_size: int = 500) -> SQLScheduleEntryList:
        """List schedule entries.

        Parameters
        ----------
        page_size : int, optional
            DB page size, by default 500

        Returns
        -------
        SQLScheduleEntryList
            Iterator of all schedule entries.  Automatically paginated DB results.
        """
        return SQLScheduleEntryList(
            page_size=page_size,
            default_sched_entries=self.default_sched_entries,
            session_maker=self._Session,
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

        with self._Session() as session:
            db_entry = session.query(SQLScheduleEntry).filter(SQLScheduleEntry.key_ == key).one_or_none()
            
        if db_entry is not None:
            return self._entry_type_registry.dejson_entry(db_entry.json_)

        raise exceptions.ScheduleEntryNotFound(messages.sched_entry_not_found_template.format(key))
 

    def delete(self, sched_entry: ScheduleEntry) -> None:
        """Delete a schedule entry from the scheduler.

        This does not delete default entries.

        Parameters
        ----------
        sched_entry : ScheduleEntry
            Scheduler entry to delete from the scheduler.
        """
        with self._Session() as session:
            session.query(SQLScheduleEntry).filter(
                SQLScheduleEntry.key_ == sched_entry.key
            ).delete()
            session.commit()


    def create_tables(self):
        """Create DB tables for the schedule entries.
        """
        SQLScheduleEntry.__table__.create(self._engine)
        SQLSchedulerLock.__table__.create(self._engine)


    @validator(
        "create_engine_kwargs"
    )
    def url_in_kwargs(cls, v: dict) -> dict:
        if "url" not in v:
            raise ValueError("'url' must be passed as an engine kwarg")

        return v
   

    