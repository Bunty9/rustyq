//! Async Rust client for the rustyq HTTP API.
//!
//! ```no_run
//! # async fn _ex() -> Result<(), rustyq_client::Error> {
//! let client = rustyq_client::Client::new("http://localhost:8080");
//! let id = client
//!     .enqueue("default", "send_email", serde_json::json!({ "to": "a@b" }))
//!     .await?;
//! println!("enqueued {id}");
//! # Ok(()) }
//! ```

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("invalid response from server: missing id")]
    MissingId,
    #[error("invalid response from server: id is not a uuid: {0}")]
    InvalidId(#[from] uuid::Error),
}

#[derive(Debug, Clone, Serialize)]
struct EnqueueReq<'a> {
    queue: &'a str,
    kind: &'a str,
    payload: serde_json::Value,
    #[serde(skip_serializing_if = "is_zero_i16")]
    priority: i16,
    #[serde(skip_serializing_if = "is_zero_i64")]
    delay_secs: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_attempts: Option<i32>,
}

/// Optional knobs for [`Client::enqueue_with`].
#[derive(Debug, Clone, Default)]
pub struct EnqueueOptions {
    pub priority: i16,
    pub delay_secs: i64,
    /// Server default (5) when `None`.
    pub max_attempts: Option<i32>,
}

/// Job status as returned by `GET /jobs/{id}`.
#[derive(Debug, Clone, Deserialize)]
pub struct JobStatus {
    pub id: Uuid,
    pub state: String,
    pub attempts: i32,
    pub max_attempts: i32,
    pub run_at: String,
    pub locked_by: Option<String>,
    pub last_error: Option<String>,
}

fn is_zero_i16(x: &i16) -> bool {
    *x == 0
}
fn is_zero_i64(x: &i64) -> bool {
    *x == 0
}

#[derive(Debug, Deserialize)]
struct EnqueueResp {
    id: String,
}

#[derive(Debug, Clone)]
pub struct Client {
    base_url: String,
    http: reqwest::Client,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            http: reqwest::Client::new(),
        }
    }

    pub fn with_http(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.into(),
            http,
        }
    }

    /// Enqueue with default priority/delay.
    pub async fn enqueue(
        &self,
        queue: &str,
        kind: &str,
        payload: serde_json::Value,
    ) -> Result<Uuid, Error> {
        self.enqueue_with(queue, kind, payload, EnqueueOptions::default())
            .await
    }

    /// Enqueue with explicit [`EnqueueOptions`].
    pub async fn enqueue_with(
        &self,
        queue: &str,
        kind: &str,
        payload: serde_json::Value,
        opts: EnqueueOptions,
    ) -> Result<Uuid, Error> {
        let req = EnqueueReq {
            queue,
            kind,
            payload,
            priority: opts.priority,
            delay_secs: opts.delay_secs,
            max_attempts: opts.max_attempts,
        };
        let resp: EnqueueResp = self
            .http
            .post(format!("{}/jobs", self.base_url))
            .json(&req)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if resp.id.is_empty() {
            return Err(Error::MissingId);
        }
        Ok(Uuid::parse_str(&resp.id)?)
    }

    /// Fetch a job's status; `Ok(None)` if the id is unknown.
    pub async fn status(&self, id: Uuid) -> Result<Option<JobStatus>, Error> {
        let resp = self
            .http
            .get(format!("{}/jobs/{id}", self.base_url))
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        Ok(Some(resp.error_for_status()?.json().await?))
    }
}
