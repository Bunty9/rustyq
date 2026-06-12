//! Built-in job handlers shipped with the rustyq worker.
//!
//! These three handlers cover the common use cases and are also used to
//! exercise the dispatch loop end-to-end in integration tests.

use rustyq_core::{Handler, HandlerFut, Job};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// noop — returns Ok(()) immediately, used for smoke tests.
// ---------------------------------------------------------------------------

pub struct Noop;

impl Handler for Noop {
    fn call(&self, _job: &Job) -> HandlerFut {
        Box::pin(async move { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// sleep — reads payload.ms (i64, default 0), sleeps that many milliseconds.
// ---------------------------------------------------------------------------

pub struct Sleep;

impl Handler for Sleep {
    fn call(&self, job: &Job) -> HandlerFut {
        let ms = job
            .payload
            .get("ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .max(0) as u64;
        Box::pin(async move {
            if ms > 0 {
                tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
            }
            Ok(())
        })
    }
}

// ---------------------------------------------------------------------------
// fail_once — fails on the first call per job id, succeeds on subsequent
// calls. Useful for testing the retry / backoff machinery.
// ---------------------------------------------------------------------------

#[derive(Default, Clone)]
pub struct FailOnce {
    /// Maps job id → number of times the handler has been called for that job.
    seen: Arc<Mutex<HashMap<Uuid, u8>>>,
}

impl FailOnce {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Handler for FailOnce {
    fn call(&self, job: &Job) -> HandlerFut {
        let seen = self.seen.clone();
        let id = job.id;
        Box::pin(async move {
            let mut map = seen.lock().expect("fail_once mutex poisoned");
            let count = map.entry(id).or_insert(0);
            if *count == 0 {
                *count = 1;
                Err(anyhow::anyhow!("synthetic fail_once"))
            } else {
                Ok(())
            }
        })
    }
}
