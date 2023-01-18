



sched_lock_acquired = "Scheduler lock acquired."
sched_lock_acquiring = "Trying to acquire the scheduler lock..."
sched_lock_creating = "Creating scheduler lock..."
sched_lock_expired = "Another scheduler's schedule lock has expired..."
sched_lock_lost = (
    "Another scheduler has acquired the lock while this one was running. "
    "This may cause performance issues for the scheduler(s) and clients. "
    "Try increasing the 'max_interval' and 'lock_timeout' parameters on the scheduler."
)
sched_lock_refreshing = "Refreshing scheduler lock..."
sched_lock_refreshed = "Scheduler lock refreshed."
sched_lock_released = "Scheduler lock released."
sched_lock_releasing = "Releasing scheduler lock..."
sched_lock_unavailable = "Another scheduler has the scheduler lock."
sched_lock_wait_template = "Waking up in {0:.3f} seconds to check scheduler lock status."

sched_entry_due_template = "Entry is due: {}.updating and saving..."
sched_entry_not_found_template = "Schedule entry with key: '{}' could not be found."
sched_entry_sending_template = "Sending entry: {}"
sched_entry_sent_template = "Schedule Entry sent: {}"

scheduler_max_iterations = "Scheduler has reached the max run iterations."
scheduler_pulling_entries = "Pulling all schedule entries..."
scheduler_shut_down = "Scheduler shutdown."
scheduler_shutting_down = "Shutting down the scheduler..."
scheduler_sleep_template = "Sleeping for {0:.3f} seconds..."
scheduler_starting = "Starting scheduler..."