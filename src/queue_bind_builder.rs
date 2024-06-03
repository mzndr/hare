//! [`QueueBindBuilder`] implementation.

use lapin::options::QueueBindOptions;
use lapin::types::FieldTable;

use crate::{BuilderArgs, Client, QueueName};

/// Bind a queue to an exchange and routing key.
pub struct QueueBindBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// [`Client`] to bind the queue with.
    client: &'a Client<S>,
    /// Name of the queue.
    name: &'a str,
    /// Name of the exchange.
    exchange_name: &'a str,
    /// Routing key.
    routing_key: &'a str,
    /// Additional options.
    opts: QueueBindOptions,
    /// Additional arguments.
    args: FieldTable,
}

impl<'a, S> QueueBindBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new instance of `QueueBindBuilder`.
    #[must_use]
    pub fn new<N>(
        client: &'a Client<S>,
        name: &'a N,
        exchange_name: &'a str,
        routing_key: &'a str,
    ) -> Self
    where
        N: QueueName,
    {
        Self {
            client,
            name: name.queue_name(),
            exchange_name,
            routing_key,
            opts: QueueBindOptions { nowait: false },
            args: FieldTable::default(),
        }
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn nowait(mut self, nowait: bool) -> Self {
        self.opts.nowait = nowait;
        self
    }

    /// Add additional options.
    #[must_use]
    pub fn opts<F>(mut self, f: F) -> Self
    where
        F: FnOnce(QueueBindOptions) -> QueueBindOptions,
    {
        self.opts = f(self.opts);
        self
    }

    /// Bind the queue.
    /// # Errors
    /// See [`BindError`].
    pub async fn bind(self) -> Result<(), BindError> {
        let chan = self
            .client
            .get_channel()
            .await
            .map_err(|err| BindError(err.into()))?;
        chan.queue_bind(
            self.name,
            self.exchange_name,
            self.routing_key,
            self.opts,
            self.args,
        )
        .await
        .map_err(|err| BindError(err.into()))
    }
}

/// Errors that can occur when binding a queue.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct BindError(#[from] anyhow::Error);

impl<'a, S> BuilderArgs for QueueBindBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    fn get_args(&mut self) -> &mut FieldTable {
        &mut self.args
    }
}
