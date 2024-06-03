//! [`QueuePurgeBuilder`] implementation.

use lapin::options::QueuePurgeOptions;
use lapin::types::MessageCount;

use crate::{Client, QueueName};

/// Purge a queue.
pub struct QueuePurgeBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// [`Client`] to purge the queue with.
    client: &'a Client<S>,
    /// Name of the queue to purge.
    name: &'a str,
    /// Purge options.
    opts: QueuePurgeOptions,
}

impl<'a, S> QueuePurgeBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new instance of `QueuePurgeBuilder`.
    #[must_use]
    pub fn new<N>(client: &'a Client<S>, name: &'a N) -> Self
    where
        N: QueueName,
    {
        Self {
            client,
            name: name.queue_name(),
            opts: QueuePurgeOptions { nowait: false },
        }
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn nowait(mut self, nowait: bool) -> Self {
        self.opts.nowait = nowait;
        self
    }

    /// Set the queue purge options.
    #[must_use]
    pub fn opts<F>(mut self, f: F) -> Self
    where
        F: FnOnce(QueuePurgeOptions) -> QueuePurgeOptions,
    {
        self.opts = f(self.opts);
        self
    }

    /// Purge the queue.
    /// # Errors
    /// See [`PurgeError`].
    pub async fn purge(self) -> Result<MessageCount, PurgeError> {
        let chan = self
            .client
            .get_channel()
            .await
            .map_err(|err| PurgeError(err.into()))?;
        chan.queue_purge(self.name, self.opts)
            .await
            .map_err(|err| PurgeError(err.into()))
    }
}

/// Any errors that can occur while purging a queue.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct PurgeError(#[from] anyhow::Error);
