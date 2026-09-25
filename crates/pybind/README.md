# rustyq (Python client)

PyO3 bindings around the rustyq HTTP API — a thin, blocking `Client` for
enqueueing jobs and checking their status from Python.

## Quick start

```bash
cd crates/pybind
maturin develop --release   # builds the wheel into your venv
```

```python
import rustyq

client = rustyq.Client("http://localhost:8080", timeout_secs=10.0)
job_id = client.enqueue("default", "send_email", {"to": "a@b"}, priority=0, delay_secs=0)
print(client.status(job_id))  # -> {"id": ..., "state": "queued", "attempts": 0, ...}
```

`payload` is any JSON-serialisable Python object (dict/list/str/number/None) —
it is serialised with `json.dumps`, not passed through as a JSON string.
Non-2xx responses raise `RuntimeError`; `status()` of an unknown job id raises
`KeyError`.
