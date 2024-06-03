//! [`QueueDeleteBuilder`] implementation.

use lapin::options::QueueDeleteOptions;
use lapin::types::MessageCount;

use crate::{Client, QueueName};

/// Delete a `Queue`.
pub struct QueueDeleteBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// The [`Client`] to delete the queue with.
    client: &'a Client<S>,
    /// Queue name.
    name: &'a str,
    /// Additional options.
    opts: QueueDeleteOptions,
}

impl<'a, S> QueueDeleteBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new `QueueDeleteBuilder` instance.
    #[must_use]
    pub fn new<N>(client: &'a Client<S>, name: &'a N) -> Self
    where
        N: QueueName,
    {
        Self {
            client,
            name: name.queue_name(),
            opts: QueueDeleteOptions {
                if_empty: false,
                if_unused: false,
                nowait: false,
            },
        }
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn if_empty(mut self, if_empty: bool) -> Self {
        self.opts.if_empty = if_empty;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn if_unused(mut self, if_unused: bool) -> Self {
        self.opts.if_unused = if_unused;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn nowait(mut self, nowait: bool) -> Self {
        self.opts.nowait = nowait;
        self
    }

    /// Set additional options.
    #[must_use]
    pub fn opts<F>(mut self, f: F) -> Self
    where
        F: FnOnce(QueueDeleteOptions) -> QueueDeleteOptions,
    {
        self.opts = f(self.opts);
        self
    }

    /// Delete the queue.
    /// # Errors
    /// See [`DeleteError`].
    pub async fn delete(self) -> Result<MessageCount, DeleteError> {
        let chan = self
            .client
            .get_channel()
            .await
            .map_err(|err| DeleteError(err.into()))?;
        chan.queue_delete(self.name, self.opts)
            .await
            .map_err(|err| DeleteError(err.into()))
    }
}

/// All errors that can happen when deleting a queue.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DeleteError(#[from] anyhow::Error);
