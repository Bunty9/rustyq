import os
import redis
from celery import Celery

# Initialize Redis connection for counting completions
r = redis.Redis.from_url(os.environ.get("COUNTER_URL", "redis://redis:6379/1"))

# Initialize Celery app
app = Celery(
    "bench",
    broker=os.environ.get("BROKER_URL", "redis://redis:6379/0"),
)

# Celery configuration
app.conf.task_ignore_result = True
app.conf.worker_prefetch_multiplier = 16
app.conf.task_acks_late = True


@app.task
def noop():
    """Trivial task that just increments the counter."""
    r.incr("bench:done")
