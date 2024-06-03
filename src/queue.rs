//! [`Queue`] definition.

use std::ops::Deref;

/// Queue, wrapper for [`lapin::Queue`].
#[derive(Debug, Clone)]
pub struct Queue {
    /// The actual queue.
    pub inner: lapin::Queue,
    /// Enable or disable dead lettering.
    pub(super) dead_lettering: bool,
}

impl Deref for Queue {
    type Target = lapin::Queue;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Unify way to get the name of a queue.
#[allow(clippy::module_name_repetitions)]
pub trait QueueName {
    /// Get the name of a queue.
    fn queue_name(&self) -> &str;
}

impl QueueName for &str {
    fn queue_name(&self) -> &str {
        self
    }
}

impl QueueName for lapin::Queue {
    fn queue_name(&self) -> &str {
        self.name().as_str()
    }
}

impl QueueName for Queue {
    fn queue_name(&self) -> &str {
        self.name().as_str()
    }
}
