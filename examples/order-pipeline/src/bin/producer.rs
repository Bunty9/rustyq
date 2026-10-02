//! A separate service that enqueues over HTTP with `rustyq-client`: no
//! database access, no shared crate with the worker beyond the job contract
//! (queue, kind, payload). Exits 0 only if the job ends `done`.

use std::time::{Duration, Instant};

use clap::Parser;
use rustyq_client::{Client, EnqueueOptions};

#[derive(Parser)]
struct Args {
    /// Base URL of a rustyq HTTP API (here: the api binary's `/queue` mount).
    #[arg(long, default_value = "http://127.0.0.1:3000/queue")]
    url: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new(Args::parse().url);

    let started = Instant::now();
    let id = client
        .enqueue_with(
            "default",
            "report.daily",
            serde_json::json!({}),
            EnqueueOptions {
                delay_secs: 2,
                priority: 5,
                ..Default::default()
            },
        )
        .await?;
    println!("enqueued report.daily {id} (delay 2s)");

    // Poll the job until it reaches a terminal state.
    let deadline = started + Duration::from_secs(60);
    loop {
        let status = client
            .status(id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {id} vanished"))?;
        if status.state == "done" || status.state == "dead" {
            let elapsed = started.elapsed();
            println!("final status: {status:?}");
            println!("observed delay: {:.1}s", elapsed.as_secs_f64());
            if status.state == "done" {
                anyhow::ensure!(
                    elapsed >= Duration::from_secs(2),
                    "job finished before its 2s delay elapsed"
                );
                return Ok(());
            }
            anyhow::bail!("job ended dead");
        }
        anyhow::ensure!(Instant::now() < deadline, "timed out waiting for job {id}");
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
