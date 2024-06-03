//! RPC implementation, a method to send an `amqp message` and wait for a response.
//! This is discouraged as its contradictory to the 'fire and forget' philosophy of
//! self contained microservices as it establishes hard dependencies betweens services.
//! RPC calls might result in other RPC calls, which chains response times and adds
//! a lot of complexity to a system. Use with care.

use std::time::Duration;

use futures::prelude::*;
use lapin::options::BasicConsumeOptions;
use lapin::types::FieldTable;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{message, Client};

/// This duration will be added to the queue expiry, which is set
/// to the RPC timeout. If the RPC timeout is `n`,
/// the response queue will expire after `n + RESPONSE_QUEUE_EXPIRE_MARGIN`.
const RESPONSE_QUEUE_EXPIRE_MARGIN: Duration = Duration::from_millis(500);

/// Build an RPC request; Send an `amqp message` and wait for a response.
/// This is discouraged as its contradictory to the 'fire and forget' philosophy of
/// self contained microservices as it establishes hard dependencies betweens services.
/// RPC calls might result in other RPC calls, which chains response times and adds
/// a lot of complexity to a system. Use with care.
///
/// The RPC functionallity works as follows:
/// * Define a temporary RPC response queue.
/// * Send a RPC request message, containing the arguments and the name of the
/// response queue, to the request queue.
/// * The RPC consumer (the other service) will handle the request message, and send its response to the
/// provided response queue, while also setting the `correlation id` of the response
/// message to the id of the request message.
/// * The call function will wait until the timeout is reached or a message with the corresponding
/// `correlation id` appears in the queue and return its contents.
pub struct RpcBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// The `Client` to send the request with.
    client: &'a Client<S>,
    /// The queue to send the request to.
    request_queue_name: &'a str,
    /// Response timeout.
    timeout_duration: Duration,
}

impl<'a, S> RpcBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new `RpcBuilder`.
    #[must_use]
    pub fn new(client: &'a Client<S>, request_queue_name: &'a str) -> Self {
        Self {
            client,
            request_queue_name,
            timeout_duration: Duration::from_secs(10),
        }
    }

    /// Defaults to 10 seconds.
    #[must_use]
    pub fn timeout_duration(mut self, timeout_duration: Duration) -> Self {
        self.timeout_duration = timeout_duration;
        self
    }

    /// Issue the RPC request.
    ///
    /// TODO: Maybe set `R` to [`Result<R, E>`] to
    /// enforce unified error handling.
    ///
    /// # Errors
    /// see [`CallError`]
    pub async fn call<A, R>(self, args: A) -> Result<R, CallError>
    where
        A: Serialize,
        R: DeserializeOwned,
    {
        let queue_uid = uuid::Uuid::new_v4();
        let response_queue = self
            .client
            .queue_declare_builder(&format!("rpc_response_queue_{queue_uid}"))
            .auto_delete(true)
            .durable(false)
            .expires(
                self.timeout_duration
                    .saturating_add(RESPONSE_QUEUE_EXPIRE_MARGIN),
            )
            .map_err(|_| CallError::TimeoutTooBig)?
            .declare()
            .await
            .map_err(|err| CallError::QueueDeclare(err.into()))?;

        let message_id = message::Id::new_v4();

        self.client
            .basic_publish_builder("", self.request_queue_name)
            .message_id(message_id)
            .reply_to(response_queue.name().clone())
            .publish(args)
            .await
            .map_err(|err| CallError::PublishRequest(err.into()))?;

        // TODO: Replace with `basic_get()` or `basic_get_builder()`.
        let chan = self
            .client
            .create_channel()
            .await
            .map_err(|err| CallError::Channel(err.into()))?;
        let mut consumer = chan
            .basic_consume(
                &response_queue.name().to_string(),
                "",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|err| CallError::ConsumeResponse(err.into()))?;

        let delivery_result_opt = tokio::select! {
            delivery_result_opt = consumer.next() => Ok(delivery_result_opt),
            () = tokio::time::sleep(self.timeout_duration) => Err(CallError::TimeoutReached)
        }?;

        let delivery = delivery_result_opt
            .ok_or(CallError::ConsumerCanceled)?
            .map_err(|err| CallError::Delivery(err.into()))?;

        let correlation_id = delivery.properties.correlation_id().clone().ok_or(
            CallError::CorrelationIdMismatch(
                "response message is missing correlation_id".to_string(),
            ),
        )?;

        if correlation_id.to_string() != message_id.to_string() {
            return Err(CallError::CorrelationIdMismatch(
                "response message correlation_id does not match".to_string(),
            ));
        }

        let payload = message::Payload::<R>::deserialize(&delivery.data)
            .map(|payload| payload.0)
            .map_err(|err| CallError::DeserializeReturn(err.into()))?;

        self.client
            .queue_purge_builder(&response_queue.name().as_str())
            .purge()
            .await
            .map_err(|err| CallError::PurgeQueue(err.into()))?;

        Ok(payload)
    }
}

/// Errors when issuing an RPC request.
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// The channel for the RPC request could not be created.
    #[error("an error occurred while creating the channel: {0}")]
    Channel(anyhow::Error),
    /// The response queue for the RPC request could not be declared.
    #[error("an error occurred while declaring the consumer queue: {0}")]
    QueueDeclare(anyhow::Error),
    /// Publishing the RPC request failed.
    #[error("an error occurred while publishing the RPC request message: {0}")]
    PublishRequest(anyhow::Error),
    /// Consuming the response failed.
    #[error("an error ocurred while consuming the RPC response message: {0}")]
    ConsumeResponse(anyhow::Error),
    /// Serializing the RPC arguments failed.
    #[error("an error ocurred while serializing the RPC arguments: {0}")]
    SerializeArguments(anyhow::Error),
    /// The response message wasn't sent in time.
    #[error("timeout reached")]
    TimeoutReached,
    /// The set timeout is too big.
    #[error("(timeout + {RESPONSE_QUEUE_EXPIRE_MARGIN:?}) milliseconds need to be smaller than u32::MAX")]
    TimeoutTooBig,
    /// The consumer was canceled before the response message has arrived or the timeout was
    /// reached.
    #[error("consumer was canceled before the response message was consumed (delivery stream returned None)")]
    ConsumerCanceled,
    /// Receiving the response message failed.
    #[error("an error ocurred while receiving the RPC response delivery: {0}")]
    Delivery(anyhow::Error),
    /// The response messages `correlation id` didn't match the request messages id.
    #[error("correlation_id mismatch: {0}")]
    CorrelationIdMismatch(String),
    /// Deserializing the response result failed.
    #[error("an error ocurred while deserializing the RPC return: {0}")]
    DeserializeReturn(anyhow::Error),
    /// Purging the response queue failed.
    #[error("purging queue failed: {0}")]
    PurgeQueue(anyhow::Error),
}
