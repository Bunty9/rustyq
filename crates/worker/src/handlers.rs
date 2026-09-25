//! Built-in job handlers shipped with the rustyq worker.
//!
//! These three handlers cover the common use cases and are also used to
//! exercise the dispatch loop end-to-end in integration tests.

use rustyq_core::{Handler, HandlerFut, Job};

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
// fail_once — fails the first attempt of each job, succeeds on retries.
// Useful for testing the retry / backoff machinery. Keyed on `job.attempts`
// (bumped by every claim) rather than in-process state, so it behaves the
// same whichever worker picks up the retry and holds no memory per job.
// ---------------------------------------------------------------------------

pub struct FailOnce;

impl Handler for FailOnce {
    fn call(&self, job: &Job) -> HandlerFut {
        let first = job.attempts <= 1;
        Box::pin(async move {
            if first {
                Err(anyhow::anyhow!("synthetic fail_once"))
            } else {
                Ok(())
            }
        })
    }
}
