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
        self.enqueue_with(queue, kind, payload, 0, 0).await
    }

    /// Full enqueue with priority + delay.
    pub async fn enqueue_with(
        &self,
        queue: &str,
        kind: &str,
        payload: serde_json::Value,
        priority: i16,
        delay_secs: i64,
    ) -> Result<Uuid, Error> {
        let req = EnqueueReq {
            queue,
            kind,
            payload,
            priority,
            delay_secs,
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
}
