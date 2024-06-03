//! Main [`Client`] implementation.
use std::ops::Deref;
use std::sync::Arc;

use lapin::{Connection, ConnectionProperties, ExchangeKind};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::prelude::*;
use crate::basic_consume_builder::ConsumerTaskHandlerError;
use crate::{
    BasicConsumeBuilder, BasicPublishBuilder, Channel, ChannelPool, ExchangeDeclareBuilder, Queue,
    QueueBindBuilder, QueueDeclareBuilder, QueueDeleteBuilder, QueueName, QueuePurgeBuilder,
    QueueUnbindBuilder, RpcBuilder,
};

/// Use this client to interface with the `RabbitMq`. This
/// client provides functionallity for all common `RabbitMq` operations
/// like registering `queues`, binding `consumers` and declaring `exchanges`.
#[derive(Debug, Clone)]
pub struct Client<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// `AMQP` connection.
    conn: Arc<Connection>,
    /// Channel Pool.
    chan_pool: ChannelPool,
    /// `AMQP` Consumers.
    pub(super) consumer_set: Arc<Mutex<JoinSet<Result<(), ConsumerTaskHandlerError>>>>,
    /// App ID for naming connections etc.
    pub(super) app_id: Arc<String>,
    /// State to share with consumers.
    pub(super) state: Arc<S>,
}

impl<S> Client<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Creates a new `RabbitMQ` client that connects to a `RabbitMQ` server via the provided
    /// connection string. It will hold the connection internally and create a pool of channels for
    /// you to use. You can temporarily get a pooled channel with the [`Self.get_channel()`]
    /// method.
    ///
    /// # Errors
    ///
    /// This function may return an error due to one of the following reasons:
    ///
    /// - A connection to the `RabbitMQ` server can not be established
    /// - The channel pool can not be created
    /// - A channel from the channel pool can not be obtained
    /// - The DLX exchange can not be declared
    pub async fn new(
        uri: impl AsRef<str>,
        props: ConnectionProperties,
        app_id: &str,
        state: S,
    ) -> Result<Self, NewError> {
        /// Prefix errors regarding the creation.
        const ERR_TRACE_PREFIX: &str = "RabbitMQ client failed";

        let conn = Arc::new(
            Connection::connect(uri.as_ref(), props.with_connection_name(app_id.into()))
                .await
                .map_err(|err| NewError::Connection(err.into()))
                .on_err(|err| error!("{ERR_TRACE_PREFIX}: {err}"))?,
        );

        let chan_pool = ChannelPool::new(conn.clone())
            .map_err(|err| NewError::ChannelPool(err.into()))
            .on_err(|err| error!("{ERR_TRACE_PREFIX}: {err}"))?;

        info!(
            "RabbitMQ client started: connected to {}",
            uri.as_ref().split('@').last().unwrap_or_default()
        );
        Ok(Self {
            conn,
            chan_pool,
            consumer_set: Arc::new(Mutex::new(JoinSet::new())),
            app_id: Arc::new(app_id.to_string()),
            state: Arc::new(state),
        })
    }

    /// # Errors
    pub async fn get_channel(&self) -> Result<Channel, GetChannelError> {
        self.chan_pool
            .get()
            .await
            .map_err(|err| GetChannelError::Other(err.into()))
    }

    /// Creates an [`ExchangeDeclareBuilder`] to declare a new exchange.
    #[must_use]
    pub fn exchange_declare_builder<'a>(
        &'a self,
        name: &'a str,
        kind: ExchangeKind,
    ) -> ExchangeDeclareBuilder<S> {
        ExchangeDeclareBuilder::new(self, name, kind)
    }

    /// Creates a [`QueueDeclareBuilder`] to declare a new queue.
    #[must_use]
    pub fn queue_declare_builder<'a>(&'a self, name: &'a str) -> QueueDeclareBuilder<S> {
        QueueDeclareBuilder::new(self, name)
    }

    /// Creates a [`QueueBindBuilder`] to bind an existing queue to an exchange.
    #[must_use]
    pub fn queue_bind_builder<'a, N>(
        &'a self,
        name: &'a N,
        exchange_name: &'a str,
        routing_key: &'a str,
    ) -> QueueBindBuilder<S>
    where
        N: QueueName,
    {
        QueueBindBuilder::new(self, name, exchange_name, routing_key)
    }

    /// Creates a [`QueueUnbindBuilder`] to unbind an existing queue from an exchange.
    #[must_use]
    pub fn queue_unbind_builder<'a, N>(
        &'a self,
        name: &'a N,
        exchange_name: &'a str,
        routing_key: &'a str,
    ) -> QueueUnbindBuilder<S>
    where
        N: QueueName,
    {
        QueueUnbindBuilder::new(self, name, exchange_name, routing_key)
    }

    /// Creates a [`QueueDeleteBuilder`] to delete an existing queue.
    #[must_use]
    pub fn queue_delete_builder<'a, N>(&'a self, name: &'a N) -> QueueDeleteBuilder<S>
    where
        N: QueueName,
    {
        QueueDeleteBuilder::new(self, name)
    }

    /// Creates a [`QueuePurgeBuilder`] to purge an existing queue.
    #[must_use]
    pub fn queue_purge_builder<'a, N>(&'a self, name: &'a N) -> QueuePurgeBuilder<S>
    where
        N: QueueName,
    {
        QueuePurgeBuilder::new(self, name)
    }

    /// Creates a [`BasicPublishBuilder`] to publish a message to an exchange.
    #[must_use]
    pub fn basic_publish_builder<'a>(
        &'a self,
        exchange_name: &'a str,
        routing_key: &'a str,
    ) -> BasicPublishBuilder<S> {
        BasicPublishBuilder::new(self, exchange_name, routing_key)
    }

    /// Creates a [`BasicConsumeBuilder`] to consume messages from a queue.
    #[must_use]
    pub fn basic_consume_builder<'a>(
        &'a self,
        queue: &'a Queue,
        consumer_tag: &'a str,
    ) -> BasicConsumeBuilder<S> {
        BasicConsumeBuilder::new(self, queue, consumer_tag)
    }

    /// Creates an [`RpcBuilder`] to execute a remote procedure call to the specififed queue.
    #[must_use]
    pub fn rpc_builder<'a>(&'a self, request_queue_name: &'a str) -> RpcBuilder<S> {
        RpcBuilder::new(self, request_queue_name)
    }

    /// Joins the previously created basic consumers. Note that consumers, which are created after
    /// this method was called, will not be joined.
    ///
    /// # Errors
    ///
    /// # Panics
    /// If the lapin connection errors.
    pub async fn completion(&self) -> Result<(), JoinBasicConsumersError> {
        /// Prefix for errors happening here duh
        const ERR_TRACE_PREFIX: &str = "a RabbitMQ client consumer failed";

        let (conn_error_sender, mut conn_error_receiver) =
            tokio::sync::mpsc::unbounded_channel::<lapin::Error>();
        self.conn.on_error(move |e| {
            conn_error_sender
                .send(e)
                .expect("connection error receiver dropped.");
        });

        let mut consumer_set = std::mem::take(&mut *self.consumer_set.lock().await);

        loop {
            tokio::select! {
                conn_error_opt = conn_error_receiver.recv() => {
                    let Some(conn_err) = conn_error_opt else {
                        tracing::warn!("lapin connection error sender dropped");
                        return Err(JoinBasicConsumersError::ConnectionErrorReceiverDropped);
                    };
                    tracing::error!("received lapin connection error: {:?}", conn_err);
                    return Err(JoinBasicConsumersError::Connection(conn_err));
                },
                join_result_opt = consumer_set.join_next(), if !consumer_set.is_empty() => {
                    let Some(join_result) = join_result_opt else {
                        continue;
                    };
                    let delivery_result = join_result
                        .map_err(|err| JoinBasicConsumersError::JoinTask(err.into()))
                        .on_err(|err| error!("{ERR_TRACE_PREFIX}: {err}"))?;
                    delivery_result
                        .map_err(|err| JoinBasicConsumersError::Consumer(err.into()))
                        .on_err(|err| error!("{ERR_TRACE_PREFIX}: {err}"))?;
                }
            }
        }
    }
}

/// Possible errors when creating the [`Client`].
#[derive(Debug, thiserror::Error)]
pub enum NewError {
    /// Connecting to the `RabbitMq` failed.
    #[error("connecting to RabbitMQ server failed: {0}")]
    Connection(anyhow::Error),
    /// Creating the channel pool failed.
    #[error("creating channel pool failed: {0}")]
    ChannelPool(anyhow::Error),
    /// Declaring the DLX-Exchange failed.
    #[error("declaring the DLX exchange failed: {0}")]
    DlxExchangeDeclare(anyhow::Error),
}

/// Errors when requesting a [`lapin::Channel`].
#[derive(Debug, thiserror::Error)]
pub enum GetChannelError {
    /// Any error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors when joining/starting bound consumers.
#[derive(Debug, thiserror::Error)]
pub enum JoinBasicConsumersError {
    /// Joining a consumer task failed.
    #[error("joining task failed: {0}")]
    JoinTask(anyhow::Error),
    /// Errors happening inside the consumer at runtime.
    #[error(transparent)]
    Consumer(#[from] anyhow::Error),

    /// The connection error receiver was dropped.
    #[error("lapin connection error listener dropped")]
    ConnectionErrorReceiverDropped,
    /// The currently held [`lapin::Connection`] has errored in any form, for
    /// example because it was interrupted.
    #[error(transparent)]
    Connection(#[from] lapin::Error),
}

impl<T> Deref for Client<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}
