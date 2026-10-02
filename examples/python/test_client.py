"""Live tests for the rustyq Python client.

Skipped unless RUSTYQ_URL is set to a running rustyq-server, e.g.:

    RUSTYQ_URL=http://127.0.0.1:18080 pytest examples/python/test_client.py
"""

import os
import uuid
from datetime import datetime, timezone

import pytest

import rustyq

RUSTYQ_URL = os.environ.get("RUSTYQ_URL")
pytestmark = pytest.mark.skipif(not RUSTYQ_URL, reason="RUSTYQ_URL not set")


@pytest.fixture
def client():
    return rustyq.Client(RUSTYQ_URL)


def test_enqueue_returns_uuid(client):
    job_id = client.enqueue("default", "noop", {"hello": "world"})
    uuid.UUID(job_id)  # raises ValueError if not a valid uuid


def test_status_of_freshly_enqueued_job(client):
    # A queue no worker drains, so the job is still `queued` when we look.
    job_id = client.enqueue(f"test-{uuid.uuid4()}", "noop", {"hello": "world"})
    status = client.status(job_id)
    assert status["state"] == "queued"
    assert status["attempts"] == 0


def test_status_of_unknown_job_raises_keyerror(client):
    with pytest.raises(KeyError):
        client.status(str(uuid.uuid4()))


def test_priority_and_delay_secs_accepted(client):
    job_id = client.enqueue("default", "noop", {"x": 1}, priority=5, delay_secs=3600)
    status = client.status(job_id)
    run_at = datetime.fromisoformat(status["run_at"].replace("Z", "+00:00"))
    assert run_at > datetime.now(timezone.utc)


def test_max_attempts_round_trips(client):
    job_id = client.enqueue(f"test-{uuid.uuid4()}", "noop", {}, max_attempts=7)
    assert client.status(job_id)["max_attempts"] == 7
    job_id = client.enqueue(f"test-{uuid.uuid4()}", "noop", {})
    assert client.status(job_id)["max_attempts"] == 5
