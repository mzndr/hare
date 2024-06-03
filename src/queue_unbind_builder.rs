//! [`QueueUnbindBuilder`] implementation.

use lapin::types::FieldTable;

use crate::{BuilderArgs, Client, QueueName};

/// Unbind a queue.
pub struct QueueUnbindBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// [`Client`] to unbind the queue with.
    client: &'a Client<S>,
    /// Queue name.
    name: &'a str,
    /// Exchange name.
    exchange_name: &'a str,
    /// Routing key.
    routing_key: &'a str,
    /// Additional arguments.
    args: FieldTable,
}

impl<'a, S> QueueUnbindBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new `QueueUnbindBuilder`
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
            args: FieldTable::default(),
        }
    }

    /// Unbind the `Queue`.
    /// # Errors
    /// See [`UnbindError`].
    pub async fn unbind(self) -> Result<(), UnbindError> {
        let chan = self
            .client
            .get_channel()
            .await
            .map_err(|err| UnbindError(err.into()))?;
        chan.queue_unbind(self.name, self.exchange_name, self.routing_key, self.args)
            .await
            .map_err(|err| UnbindError(err.into()))
    }
}

/// Queue unbinding failed.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct UnbindError(#[from] anyhow::Error);

impl<'a, S> BuilderArgs for QueueUnbindBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    fn get_args(&mut self) -> &mut FieldTable {
        &mut self.args
    }
}
