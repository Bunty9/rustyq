//! PyO3 client — thin blocking wrapper around the rustyq enqueue API.
//!
//! Built into a Python wheel via `maturin build` (see `pyproject.toml`).
//! Import as `import rustyq; c = rustyq.Client("http://localhost:8080")`.

use pyo3::prelude::*;

#[pyclass]
struct Client {
    base_url: String,
    client: reqwest::blocking::Client,
}

#[pymethods]
impl Client {
    #[new]
    fn new(base_url: String) -> Self {
        Self {
            base_url,
            client: reqwest::blocking::Client::new(),
        }
    }

    fn enqueue(&self, queue: &str, kind: &str, payload: &str) -> PyResult<String> {
        let parsed: serde_json::Value = serde_json::from_str(payload)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?;
        let resp: serde_json::Value = self
            .client
            .post(format!("{}/jobs", self.base_url))
            .json(&serde_json::json!({
                "queue": queue,
                "kind": kind,
                "payload": parsed,
            }))
            .send()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?
            .json()
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        Ok(resp["id"].as_str().unwrap_or("").to_string())
    }
}

#[pymodule]
fn rustyq(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Client>()?;
    Ok(())
}
