//! [`QueueBindBuilder`] implementation.

use lapin::options::ExchangeDeclareOptions;
use lapin::types::FieldTable;
use lapin::ExchangeKind;

use crate::{BuilderArgs, Client};

/// Declare an `Exchange`.
pub struct ExchangeDeclareBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// The [`Client`] to declare the exchange with.
    client: &'a Client<S>,
    /// Name of the exchange.
    name: &'a str,
    /// Kind of the exchange.
    kind: ExchangeKind,
    /// Additional declare options.
    opts: ExchangeDeclareOptions,
    /// Additional declare args.
    args: FieldTable,
}

impl<'a, S> ExchangeDeclareBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new instance of `ExchangeDeclareBuilder`.
    #[must_use]
    pub fn new(client: &'a Client<S>, name: &'a str, kind: ExchangeKind) -> Self {
        Self {
            client,
            name,
            kind,
            opts: ExchangeDeclareOptions {
                auto_delete: false,
                durable: true,
                internal: false,
                nowait: false,
                passive: false,
            },
            args: FieldTable::default(),
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
    pub fn internal(mut self, internal: bool) -> Self {
        self.opts.internal = internal;
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

    /// Add additional options.
    #[must_use]
    pub fn opts<F>(mut self, f: F) -> Self
    where
        F: FnOnce(ExchangeDeclareOptions) -> ExchangeDeclareOptions,
    {
        self.opts = f(self.opts);
        self
    }

    /// Declare the queue.
    /// # Errors
    /// See [`DeclareError`].
    pub async fn declare(self) -> Result<(), DeclareError> {
        let chan = self
            .client
            .get_channel()
            .await
            .map_err(|err| DeclareError(err.into()))?;
        chan.exchange_declare(self.name, self.kind, self.opts, self.args)
            .await
            .map_err(|err| DeclareError(err.into()))
    }
}

/// Errors that can occur when declaring an exchange.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct DeclareError(#[from] anyhow::Error);

impl<'a, S> BuilderArgs for ExchangeDeclareBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    fn get_args(&mut self) -> &mut FieldTable {
        &mut self.args
    }
}
