import sys
import os
import redis
from tasks import app, noop

# Get N from argv (default 20000)
n = int(sys.argv[1]) if len(sys.argv) > 1 else 20000

# Initialize Redis connection for counter
r = redis.Redis.from_url(os.environ.get("COUNTER_URL", "redis://redis:6379/1"))

# Reset counter to 0
r.set("bench:done", 0)

# Enqueue N tasks
for _ in range(n):
    noop.delay()

print(f"Enqueued {n} tasks")
