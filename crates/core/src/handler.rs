//! Handler trait and registry for rustyq job dispatch.
//!
//! A `Handler` is any type that can process a `Job` asynchronously. The
//! `Registry` maps job kinds to their handlers, and `dispatch` is called by
//! the `Worker` to route each claimed job to the correct handler.

use crate::Job;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// The return type of a handler call: a pinned, boxed, Send future.
pub type HandlerFut = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

/// Trait for job handlers. Implement this or use the blanket impl for
/// `Fn(&Job) -> HandlerFut` closures.
pub trait Handler: Send + Sync + 'static {
    fn call(&self, job: &Job) -> HandlerFut;
}

/// Blanket implementation for closures that match the `Fn(&Job) -> HandlerFut`
/// signature. This allows registering plain async closures/functions without
/// wrapping them in a struct.
impl<F> Handler for F
where
    F: Fn(&Job) -> HandlerFut + Send + Sync + 'static,
{
    fn call(&self, job: &Job) -> HandlerFut {
        (self)(job)
    }
}

/// A built and frozen registry that maps job kind strings to handlers.
/// Clone is cheap — the inner map is behind an `Arc`.
#[derive(Default, Clone)]
pub struct Registry {
    map: Arc<HashMap<String, Arc<dyn Handler>>>,
}

impl Registry {
    /// Start building a new `Registry`.
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder::default()
    }

    /// Dispatch a job to its registered handler.
    ///
    /// Returns an error future if no handler is registered for `job.kind`.
    pub fn dispatch(&self, job: &Job) -> HandlerFut {
        match self.map.get(&job.kind) {
            Some(handler) => handler.call(job),
            None => {
                let kind = job.kind.clone();
                Box::pin(async move { Err(anyhow::anyhow!("no handler for kind '{}'", kind)) })
            }
        }
    }
}

/// Builder for `Registry`. Accumulates handlers before freezing into a
/// `Registry`.
#[derive(Default)]
pub struct RegistryBuilder {
    map: HashMap<String, Arc<dyn Handler>>,
}

impl RegistryBuilder {
    /// Register a handler for `kind`. Overwrites any previous handler for the
    /// same kind.
    pub fn register<H: Handler>(mut self, kind: impl Into<String>, h: H) -> Self {
        self.map.insert(kind.into(), Arc::new(h));
        self
    }

    /// Freeze the builder into a `Registry`.
    pub fn build(self) -> Registry {
        Registry {
            map: Arc::new(self.map),
        }
    }
}
