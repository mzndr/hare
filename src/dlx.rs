//! Deadlettering implementation.
use lapin::message::Delivery;
use serde::{Deserialize, Serialize};

/// Deadlettering exchange, the `dlx handler` is subscribed to.
pub const EXCHANGE_NAME: &str = "amq.direct";
/// Routing key for dead messages the `dlx` handler is subscribed to.
pub const ROUTING_KEY_DEAD_LETTER: &str = "dlx.dead_letter";
/// Routing key for message errors the `dlx` handler is subscribed to.
pub const ROUTING_KEY_ERROR: &str = "dlx.error";
/// Argument for queues to set their deadletter exchange.
pub const QUEUE_ARGUMENT_KEY_EXCHANGE_NAME: &str = "x-dead-letter-exchange";
/// Argument for queues to set their deadletter routing key.
pub const QUEUE_ARGUMENT_KEY_ROUTING_KEY: &str = "x-dead-letter-routing-key";

/// Payload for error messages for the `dlx handler` to pick up.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorData {
    /// Message id and origin queue of the message
    /// that resulten in an error.
    pub origin: ErrorDataOrigin,
    /// The error itself. Contains language specific
    /// error information.
    pub error: ErrorDataError,
}

impl ErrorData {
    /// Create a new instance.
    pub fn new<E>(queue_name: String, delivery: &Delivery, err: &E) -> Self
    where
        E: std::error::Error,
    {
        let message_id = delivery
            .properties
            .message_id()
            .clone()
            .map(|s| s.to_string());
        Self {
            origin: ErrorDataOrigin {
                message_id,
                queue_name,
            },
            error: ErrorDataError::Rs {
                name: std::any::type_name::<E>().to_string(),
                message: err.to_string(),
            },
        }
    }
}

/// Describe where an error originated from.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorDataOrigin {
    /// Message that resulted in said error.
    pub message_id: Option<String>,
    /// Queue the message got consumed from.
    pub queue_name: String,
}

/// Language specific error data.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "lang", rename_all = "camelCase")]
#[non_exhaustive]
pub enum ErrorDataError {
    /// Rust errors.
    #[serde(rename_all = "camelCase")]
    Rs {
        /// Error name.
        name: String,
        /// Error message.
        message: String,
    },
    /// JavaScript errors.
    #[serde(rename_all = "camelCase")]
    Js {
        /// Error name.
        name: String,
        /// Error message.
        message: String,
        /// Error stacktrace.
        stacktrace: String,
    },
}
