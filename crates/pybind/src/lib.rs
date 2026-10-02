//! PyO3 client — thin blocking wrapper around the rustyq enqueue API.
//!
//! Built into a Python wheel via `maturin build` (see `pyproject.toml`).
//! Import as `import rustyq; c = rustyq.Client("http://localhost:8080")`.

#![allow(clippy::useless_conversion)]

use pyo3::exceptions::{PyKeyError, PyRuntimeError};
use pyo3::prelude::*;

#[pyclass]
struct Client {
    base_url: String,
    client: reqwest::blocking::Client,
}

#[pymethods]
impl Client {
    #[new]
    #[pyo3(signature = (base_url, *, timeout_secs=30.0))]
    fn new(base_url: String, timeout_secs: f64) -> PyResult<Self> {
        let client = reqwest::blocking::Client::builder()
            .timeout(
                std::time::Duration::try_from_secs_f64(timeout_secs)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            )
            .build()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(Self { base_url, client })
    }

    /// Enqueue a job. `payload` is any JSON-serialisable Python object
    /// (dict/list/str/number/None), serialised with Python's own `json.dumps`.
    #[pyo3(signature = (queue, kind, payload=None, *, priority=0, delay_secs=0, max_attempts=None))]
    #[allow(clippy::too_many_arguments)]
    fn enqueue(
        &self,
        py: Python<'_>,
        queue: &str,
        kind: &str,
        payload: Option<Bound<'_, PyAny>>,
        priority: i16,
        delay_secs: i64,
        max_attempts: Option<i32>,
    ) -> PyResult<String> {
        let json = py.import("json")?;
        let payload = payload.unwrap_or_else(|| py.None().into_bound(py));
        let payload_str: String = json.call_method1("dumps", (payload,))?.extract()?;
        let parsed: serde_json::Value = serde_json::from_str(&payload_str)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        let mut body = serde_json::json!({
            "queue": queue,
            "kind": kind,
            "payload": parsed,
            "priority": priority,
            "delay_secs": delay_secs,
        });
        if let Some(n) = max_attempts {
            body["max_attempts"] = n.into();
        }

        let base_url = self.base_url.clone();
        let client = self.client.clone();
        let text = py.detach(move || -> PyResult<String> {
            let resp = client
                .post(format!("{base_url}/jobs"))
                .json(&body)
                .send()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let status = resp.status();
            let text = resp
                .text()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            if !status.is_success() {
                return Err(PyRuntimeError::new_err(format!(
                    "rustyq server returned {status}: {text}"
                )));
            }
            Ok(text)
        })?;

        let resp: serde_json::Value =
            serde_json::from_str(&text).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        resp["id"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| PyRuntimeError::new_err("rustyq server response missing \"id\""))
    }

    /// Fetch job status as a dict. Raises `KeyError` if the job id is unknown.
    fn status<'py>(&self, py: Python<'py>, job_id: &str) -> PyResult<Bound<'py, PyAny>> {
        let base_url = self.base_url.clone();
        let client = self.client.clone();
        let job_id_owned = job_id.to_string();
        let (status, text) = py.detach(move || -> PyResult<(u16, String)> {
            let resp = client
                .get(format!("{base_url}/jobs/{job_id_owned}"))
                .send()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let status = resp.status().as_u16();
            let text = resp
                .text()
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            Ok((status, text))
        })?;

        if status == 404 {
            return Err(PyKeyError::new_err(job_id.to_string()));
        }
        if !(200..300).contains(&status) {
            return Err(PyRuntimeError::new_err(format!(
                "rustyq server returned {status}: {text}"
            )));
        }

        let json = py.import("json")?;
        let obj = json.call_method1("loads", (text,))?;
        Ok(obj)
    }
}

#[pymodule]
fn rustyq(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    Ok(())
}
