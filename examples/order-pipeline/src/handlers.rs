//! The four job handlers. Each is a struct holding what it needs (a `PgPool`
//! clone) and implements `rustyq_core::Handler`.
//!
//! Rules every handler here follows, because rustyq is at-least-once:
//!   1. A job may run more than once (retry, crashed worker, lock timeout),
//!      so side effects must be idempotent.
//!   2. Return a plain error for "might work next time" (rustyq retries with
//!      backoff) and `permanent(..)` for "will never work" (straight to `dead`).
//!   3. `Handler::call` gets `&Job` but must return a `'static` future, so
//!      clone/deserialize what you need *before* the `async move` block.

use anyhow::Context;
use rustyq_core::{permanent, Handler, HandlerFut, Job};
use sqlx::PgPool;

use crate::{ChargePayload, EmailPayload, FraudPayload};

/// `email.order_confirmation`: "send" the confirmation email.
pub struct SendConfirmation {
    pub pool: PgPool,
}

impl Handler for SendConfirmation {
    fn call(&self, job: &Job) -> HandlerFut {
        // Permanent error: a payload that does not parse will not parse on the
        // next attempt either, so do not burn retries on it.
        let payload = match job.payload_as::<EmailPayload>() {
            Ok(p) => p,
            Err(e) => {
                return Box::pin(async move {
                    Err(permanent(
                        anyhow::Error::new(e).context("bad email payload"),
                    ))
                })
            }
        };
        let pool = self.pool.clone();
        Box::pin(async move {
            let email: String = sqlx::query_scalar("SELECT email FROM orders WHERE id = $1")
                .bind(payload.order_id)
                .fetch_one(&pool)
                .await
                .context("load order")?;

            // A real handler would call an email provider here.
            tracing::info!(order_id = %payload.order_id, %email, "sending order confirmation");

            // Idempotent side effect: if this job runs twice (at-least-once),
            // the second INSERT hits the primary key and does nothing, so the
            // recorded outcome is "sent exactly once". With a real provider,
            // pass `order_id` as the provider-side idempotency key as well.
            sqlx::query(
                "INSERT INTO sent_emails (order_id, template) VALUES ($1, 'order_confirmation') \
                 ON CONFLICT DO NOTHING",
            )
            .bind(payload.order_id)
            .execute(&pool)
            .await
            .context("record sent email")?;
            Ok(())
        })
    }
}

/// `payment.charge`: charge the customer through a (simulated, flaky) gateway.
pub struct ChargeCustomer {
    pub pool: PgPool,
}

impl Handler for ChargeCustomer {
    fn call(&self, job: &Job) -> HandlerFut {
        let payload = match job.payload_as::<ChargePayload>() {
            Ok(p) => p,
            Err(e) => {
                return Box::pin(async move {
                    Err(permanent(
                        anyhow::Error::new(e).context("bad charge payload"),
                    ))
                })
            }
        };
        let pool = self.pool.clone();
        // `attempts` is incremented when a job is claimed, so the first run
        // sees 1.
        let attempt = job.attempts;
        Box::pin(async move {
            // Transient error: a plain `Err` makes rustyq requeue the job with
            // exponential backoff (2^attempts seconds: 2 s after the first
            // failure, 4 s after the second, ...) until `max_attempts`, after
            // which it goes to `dead`. The demo fails the first attempt of
            // every charge to show this.
            if attempt == 1 {
                anyhow::bail!("payment gateway timeout");
            }

            // Idempotency key = order_id (the `charges` primary key). If a
            // previous run committed the charge but the worker died before
            // rustyq marked the job done, the retry inserts nothing and just
            // re-asserts `paid`. The charge row and the status change commit
            // together or not at all.
            let mut tx = pool.begin().await?;
            sqlx::query(
                "INSERT INTO charges (order_id, amount_cents) VALUES ($1, $2) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(payload.order_id)
            .bind(payload.amount_cents)
            .execute(&mut *tx)
            .await
            .context("record charge")?;
            sqlx::query("UPDATE orders SET status = 'paid' WHERE id = $1")
                .bind(payload.order_id)
                .execute(&mut *tx)
                .await
                .context("mark order paid")?;
            tx.commit().await?;
            tracing::info!(order_id = %payload.order_id, attempt, "charged");
            Ok(())
        })
    }
}

/// `fraud.review`: only the demo enqueues it, with a deliberately malformed
/// payload, to show a permanent failure.
pub struct FraudReview;

impl Handler for FraudReview {
    fn call(&self, job: &Job) -> HandlerFut {
        match job.payload_as::<FraudPayload>() {
            // `permanent()` => the job goes to `dead` on this attempt even
            // though `max_attempts` allows more, and `last_error` records why.
            Err(e) => Box::pin(async move {
                Err(permanent(
                    anyhow::Error::new(e).context("bad fraud payload"),
                ))
            }),
            Ok(p) => Box::pin(async move {
                tracing::info!(order_id = %p.order_id, "fraud review passed");
                Ok(())
            }),
        }
    }
}

/// `report.daily`: aggregate orders into `daily_reports`. Enqueued with a
/// delay, so it first becomes runnable `delay` seconds after enqueue.
pub struct DailyReport {
    pub pool: PgPool,
}

impl Handler for DailyReport {
    fn call(&self, _job: &Job) -> HandlerFut {
        let pool = self.pool.clone();
        Box::pin(async move {
            // Not strictly idempotent: a re-run appends a second snapshot
            // row, which is harmless for this table. If it mattered, key the
            // row on the report date and use ON CONFLICT DO NOTHING.
            sqlx::query(
                "INSERT INTO daily_reports (order_count, revenue_cents) \
                 SELECT count(*), COALESCE(sum(amount_cents), 0) FROM orders",
            )
            .execute(&pool)
            .await
            .context("write report")?;
            tracing::info!("daily report generated");
            Ok(())
        })
    }
}
