"""Before/after: swapping a Celery task queue for rustyq.

Handlers no longer run in-process — a Rust worker (see `crates/worker`)
polls the queue and dispatches by `kind`, so the "task body" moves from
Python into a Rust handler registered under that same `kind` string.
This file only shows the *call site* change (enqueueing work), not the
handler side.
"""

# --- Before: Celery -----------------------------------------------------
#
# from celery import Celery
#
# app = Celery("myapp", broker="redis://localhost:6379/0")
#
#
# @app.task
# def send_email(to, subject):
#     ...  # handler body runs in a Celery worker process
#
#
# send_email.delay("a@b", "welcome")

# --- After: rustyq --------------------------------------------------------

import rustyq

client = rustyq.Client("http://localhost:8080")

# No @app.task decorator: the handler for kind="send_email" is a Rust
# function registered in the worker binary, not this Python module.
# enqueue() just POSTs the job; a running rustyq worker picks it up.
job_id = client.enqueue("default", "send_email", {"to": "a@b", "subject": "welcome"})

# Poll for completion instead of Celery's AsyncResult:
status = client.status(job_id)
print(status["state"])  # "queued" | "running" | "done" | "failed"
