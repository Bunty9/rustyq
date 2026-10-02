#!/usr/bin/env python3
"""Python producer using the `rustyq` wheel (pip install rustyq, or
`maturin develop -m crates/pybind/Cargo.toml` from this repo).

Talks HTTP to rustyq's API, which the api binary mounts under /queue.
Exits non-zero if any expectation fails.
"""
import os
import sys
import time
import uuid

import rustyq

client = rustyq.Client(os.environ.get("RUSTYQ_URL", "http://127.0.0.1:3000/queue"))


def wait_for(job_id, states, timeout=60.0):
    """Poll status() until the job reaches one of `states`."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = client.status(job_id)
        if st["state"] in states:
            return st
        time.sleep(0.25)
    sys.exit(f"FAIL: job {job_id} did not reach {states} within {timeout}s")


def expect(cond, msg):
    if not cond:
        sys.exit(f"FAIL: {msg}")
    print(f"ok: {msg}")


# 1. A malformed payload: the fraud.review handler cannot parse it and returns
#    permanent(..), so the job goes straight to `dead` even though we allowed
#    3 attempts.
job_id = client.enqueue(
    "default", "fraud.review", {"order_id": "not-a-uuid"}, max_attempts=3
)
st = wait_for(job_id, {"dead", "done"})
expect(st["state"] == "dead", f"fraud.review is dead (state={st['state']})")
expect(st["attempts"] == 1, f"permanent error skipped retries (attempts={st['attempts']})")
print("last_error:", st["last_error"])

# 2. A valid job.
job_id = client.enqueue("default", "report.daily", {})
st = wait_for(job_id, {"done", "dead"})
expect(st["state"] == "done", f"report.daily is done (state={st['state']})")

# 3. Unknown ids raise KeyError.
try:
    client.status(str(uuid.uuid4()))
except KeyError:
    print("ok: status() of an unknown id raises KeyError")
else:
    sys.exit("FAIL: expected KeyError for an unknown job id")
