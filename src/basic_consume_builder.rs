//! Provide the builder pattern for building a `RabbitMq` consumer.

use std::sync::Arc;
use std::time::Duration;

use futures::prelude::*;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicNackOptions};
use lapin::types::FieldTable;
use lapin::Consumer;
use tokio::task::{self, JoinSet};
use tokio::time::Instant;
use tracing::{debug, debug_span, error, info, instrument, Instrument};

use crate::prelude::*;
use crate::consumer_handler_result::ConsumerHandlerError;
use crate::{dlx, BuilderArgs, Client, ConsumerHandler, Queue};
use crate::util;

/// The builder
pub struct BasicConsumeBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// The `RabbitMq` client.
    client: &'a Client<S>,
    /// The [`Queue`] to consume on.
    queue: &'a Queue,
    /// The consumer tag the consumer gets registered on.
    consumer_tag: &'a str,
    /// Lapins basic consume options.
    opts: BasicConsumeOptions,
    /// Lapins field table.
    args: FieldTable,
    /// Timeout/abort consumer operations.
    timeout: Duration,
    /// Consume messages in order, or in parallel.
    in_parallel: bool,
}

impl<'a, S> BasicConsumeBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Creates a new [`BasicConsumeBuilder`] instance.
    #[must_use]
    pub fn new(client: &'a Client<S>, queue: &'a Queue, consumer_tag: &'a str) -> Self {
        Self {
            client,
            queue,
            consumer_tag,
            opts: BasicConsumeOptions {
                exclusive: false,
                no_ack: false,
                no_local: false,
                nowait: false,
            },
            args: FieldTable::default(),
            timeout: Duration::from_secs(300),
            in_parallel: true,
        }
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn exclusive(mut self, exclusive: bool) -> Self {
        self.opts.exclusive = exclusive;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn no_local(mut self, no_local: bool) -> Self {
        self.opts.no_local = no_local;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn nowait(mut self, nowait: bool) -> Self {
        self.opts.nowait = nowait;
        self
    }

    /// Defaults to 5 minutes.
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Defaults to `true`.
    #[must_use]
    pub fn in_parallel(mut self, in_parallel: bool) -> Self {
        self.in_parallel = in_parallel;
        self
    }

    /// Provides additional options for the consumer.
    #[must_use]
    pub fn opts<F>(mut self, f: F) -> Self
    where
        F: FnOnce(BasicConsumeOptions) -> BasicConsumeOptions,
    {
        self.opts = f(self.opts);
        self
    }

    /// Finishes the build process and consumes `self`. Creates
    /// the consumer on the provided configuration.
    #[instrument(
        name = "consumer"
        skip_all,
        fields(
            queue_name = %self.queue.name().as_str(),
            consumer_tag = %self.consumer_tag,
            in_parallel = %self.in_parallel
        )
    )]
    pub async fn consume<H, T>(self, handler: H) -> Result<Consumer, ConsumeError>
    where
        H: ConsumerHandler<S, T>,
        T: 'static,
    {
        let chan = self
            .client
            .create_channel()
            .await
            .map_err(|err| ConsumeError(err.into()))?;
        let consumer = chan
            .basic_consume(
                self.queue.name().as_str(),
                self.consumer_tag,
                self.opts,
                self.args,
            )
            .await
            .map_err(|err| ConsumeError(err.into()))?;

        self.client.consumer_set.lock().await.spawn(
            consumer_task_handler(
                self.client.clone(),
                self.queue.name().to_string(),
                self.queue.dead_lettering,
                self.timeout,
                self.in_parallel,
                handler,
                consumer.clone(),
            )
            .in_current_span(),
        );
        Ok(consumer)
    }
}

/// Error wrapper for this module.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ConsumeError(#[from] anyhow::Error);

impl<'a, S> BuilderArgs for BasicConsumeBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    fn get_args(&mut self) -> &mut FieldTable {
        &mut self.args
    }
}

/// Consumer task, handle incoming deliveries.
/// Also handle signals like sigterm.
async fn consumer_task_handler<S, H, T>(
    client: Client<S>,
    queue_name: String,
    dead_lettering: bool,
    timeout: Duration,
    in_parallel: bool,
    handler: H,
    mut consumer: Consumer,
) -> Result<(), ConsumerTaskHandlerError>
where
    S: Clone + Send + Sync + 'static,
    H: ConsumerHandler<S, T>,
    T: 'static,
{
    /// Tracing prefix for errors.
    const ERR_TRACE_PREFIX: &str = "consumer failed";

    info!("consumer started");

    let mut delivery_set = JoinSet::<()>::new();
    loop {
        tokio::select! {
            signal_stop_result = util::signal_stop() => match signal_stop_result {
                Ok(()) => break,
                Err(err) => {
                    return Err(ConsumerTaskHandlerError::StopSignal(err.into()))
                        .on_err(|err| error!("{ERR_TRACE_PREFIX}: {err}"));
                }
            },
            delivery_result_opt = consumer.next() => {
                let Some(delivery_result) = delivery_result_opt else {
                    return Err(ConsumerTaskHandlerError::Consumer(anyhow::format_err!(
                        "consumer has stopped for an unknown reason"
                    )))
                    .on_err(|err| error!("{ERR_TRACE_PREFIX}: {err}"));
                };
                let delivery = match delivery_result {
                    Ok(delivery) => Arc::new(delivery),
                    Err(err) => {
                        return Err(ConsumerTaskHandlerError::Consumer(err.into()))
                            .on_err(|err| error!("{ERR_TRACE_PREFIX}: {err}"));
                    }
                };
                let client = client.clone();
                let queue_name = queue_name.clone();
                let handler = handler.clone();

                let delivery_span = debug_span!(
                    "delivery",
                    msg_id = %delivery
                        .properties
                        .message_id()
                        .clone()
                        .map(|s| s.to_string())
                        .unwrap_or_default(),
                    delivery_tag = %delivery.delivery_tag,
                );
                let delivery_task_handler = delivery_task_handler(
                    client,
                    queue_name,
                    dead_lettering,
                    timeout,
                    handler,
                    delivery,
                )
                .instrument(delivery_span);
                if in_parallel {
                    delivery_set.spawn(delivery_task_handler);
                } else {
                    std::mem::drop(task::spawn(delivery_task_handler).await);
                }
            },
            Some(_) = delivery_set.join_next() => (),
        }
    }
    while delivery_set.join_next().await.is_some() {}

    info!("consumer shut down");
    Ok(())
}

/// Errors that can occur when handling the consumer task.
#[derive(Debug, thiserror::Error)]
pub(super) enum ConsumerTaskHandlerError {
    /// A stop signal was received.
    #[error("stop signal failed: {0}")]
    StopSignal(anyhow::Error),
    /// The consumer has errored.
    #[error(transparent)]
    Consumer(#[from] anyhow::Error),
}

/// Handle incoming deliveries.
async fn delivery_task_handler<S, H, T>(
    client: Client<S>,
    queue_name: String,
    dead_lettering: bool,
    timeout: Duration,
    handler: H,
    delivery: Arc<Delivery>,
) where
    S: Clone + Send + Sync + 'static,
    H: ConsumerHandler<S, T>,
    T: 'static,
{
    let start = Instant::now();
    debug!("started processing delivery");

    let mut was_acked = false;
    'handler: {
        let handler_res =
            tokio::time::timeout(timeout, handler.call(client.clone(), delivery.clone()))
                .await
                .unwrap_or(Err(ConsumerHandlerError::Timeout(timeout)));
        match handler_res {
            Ok(()) => {
                if let Err(err) = delivery.ack(BasicAckOptions::default()).await {
                    error!("acking delivery failed: {err}");
                    break 'handler;
                }
                was_acked = true;
            }
            Err(err) => {
                error!("handler failed: {err}");
                if let Err(err) = delivery.nack(BasicNackOptions::default()).await {
                    error!("nacking delivery failed: {err}");
                    break 'handler;
                }
                if !dead_lettering || delivery.properties.message_id().is_none() {
                    break 'handler;
                }
                if let Err(err) = client
                    .basic_publish_builder(dlx::EXCHANGE_NAME, dlx::ROUTING_KEY_ERROR)
                    .publish(dlx::ErrorData::new(queue_name, &delivery, &err))
                    .await
                {
                    error!("publishing error data to DLX failed: {err}");
                }
            }
        }
    }

    debug!(
        latency = ?start.elapsed(),
        %was_acked,
        "finished processing delivery",
    );
}
