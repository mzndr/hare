//! [`QueueDeclareBuilder`] implementation.

use std::time::Duration;

use lapin::options::QueueDeclareOptions;
use lapin::types::FieldTable;

use crate::{dlx, BuilderArgs, Client, Queue};

/// Key name for queue expiry.
const XARGS_EXPIRES: &str = "x-expires";

/// Declare a queue.
pub struct QueueDeclareBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// The [`Client`] to declare the queue with.
    client: &'a Client<S>,
    /// The queues name.
    name: &'a str,
    /// Queue declare options.
    opts: QueueDeclareOptions,
    /// Additional args.
    args: FieldTable,
    /// Enable or disable dead lettering.
    dead_lettering: bool,
}

impl<'a, S> QueueDeclareBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new instance of `QueueDeclareBuilder`
    #[must_use]
    pub fn new(client: &'a Client<S>, name: &'a str) -> Self {
        Self {
            client,
            name,
            opts: QueueDeclareOptions {
                auto_delete: false,
                durable: true,
                exclusive: false,
                nowait: false,
                passive: false,
            },
            args: FieldTable::default(),
            dead_lettering: true,
        }
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn auto_delete(mut self, auto_delete: bool) -> Self {
        self.opts.auto_delete = auto_delete;
        self
    }

    /// Defaults to `true`.
    #[must_use]
    pub fn durable(mut self, durable: bool) -> Self {
        self.opts.durable = durable;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.opts.exclusive = exclusive;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn nowait(mut self, nowait: bool) -> Self {
        self.opts.nowait = nowait;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn passive(mut self, passive: bool) -> Self {
        self.opts.passive = passive;
        self
    }

    /// Defaults to `true`.
    #[must_use]
    pub fn dead_lettering(mut self, dead_lettering: bool) -> Self {
        self.dead_lettering = dead_lettering;
        self
    }

    /// Provide aditional [`QueueDeclareOptions`].
    #[must_use]
    pub fn opts<F>(mut self, f: F) -> Self
    where
        F: FnOnce(QueueDeclareOptions) -> QueueDeclareOptions,
    {
        self.opts = f(self.opts);
        self
    }

    /// Not set by default.
    ///
    /// # Errors
    ///
    /// When the duration millis are bigger than `u32::MAX`.
    pub fn expires(self, expires: Duration) -> Result<Self, DeclareError> {
        let millis: u32 =
            TryInto::<u32>::try_into(expires.as_millis()).map_err(|e| DeclareError(e.into()))?;
        Ok(self.arg_u32(XARGS_EXPIRES, millis))
    }

    /// Declare the queue.
    /// # Errors
    /// See #[`DeclareError`].
    pub async fn declare(mut self) -> Result<Queue, DeclareError> {
        if self.dead_lettering {
            self = self
                .arg_str(dlx::QUEUE_ARGUMENT_KEY_EXCHANGE_NAME, dlx::EXCHANGE_NAME)
                .arg_str(
                    dlx::QUEUE_ARGUMENT_KEY_ROUTING_KEY,
                    dlx::ROUTING_KEY_DEAD_LETTER,
                );
        }

        let chan = self
            .client
            .get_channel()
            .await
            .map_err(|err| DeclareError(err.into()))?;
        let queue = chan
            .queue_declare(self.name, self.opts, self.args)
            .await
            .map_err(|err| DeclareError(err.into()))?;

        Ok(Queue {
            inner: queue,
            dead_lettering: self.dead_lettering,
        })
    }
}

/// Errors that can occur when declaring a queue.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DeclareError(#[from] anyhow::Error);

impl<'a, S> BuilderArgs for QueueDeclareBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    fn get_args(&mut self) -> &mut FieldTable {
        &mut self.args
    }
}
