//! [`BasicPublishBuilder`] implementation.

use chrono::Utc;
use lapin::options::BasicPublishOptions;
use lapin::publisher_confirm::PublisherConfirm;
use lapin::types::{FieldTable, ShortString};
use lapin::BasicProperties;
use serde::Serialize;

use crate::{message, Client};

/// Publish a serializable data to an exchange.
pub struct BasicPublishBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// The [`Client`] to publish the message with.
    client: &'a Client<S>,
    /// The exchange to publish the message on.
    exchange_name: &'a str,
    /// The routing key to publish the message with.
    routing_key: &'a str,
    /// Additional publish options.
    opts: BasicPublishOptions,
    /// Message properties.
    props: BasicProperties,
}

impl<'a, S> BasicPublishBuilder<'a, S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Create a new `QueueDeleteBuilder` instance.
    #[must_use]
    pub fn new(client: &'a Client<S>, exchange_name: &'a str, routing_key: &'a str) -> Self {
        #[allow(clippy::cast_sign_loss)]
        Self {
            client,
            exchange_name,
            routing_key,
            opts: BasicPublishOptions {
                immediate: false,
                mandatory: false,
            },
            props: BasicProperties::default()
                .with_app_id(client.app_id.to_string().into())
                .with_message_id(message::Id::new_v4().to_string().into())
                .with_timestamp(Utc::now().timestamp() as u64),
        }
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn immediate(mut self, immediate: bool) -> Self {
        self.opts.immediate = immediate;
        self
    }

    /// Defaults to `false`.
    #[must_use]
    pub fn mandatory(mut self, mandatory: bool) -> Self {
        self.opts.mandatory = mandatory;
        self
    }

    /// Add additional publish options.
    #[must_use]
    pub fn opts<F>(mut self, f: F) -> Self
    where
        F: FnOnce(BasicPublishOptions) -> BasicPublishOptions,
    {
        self.opts = f(self.opts);
        self
    }

    /// Defaults to the `app_id` set on the client.
    #[must_use]
    pub fn app_id(mut self, app_id: String) -> Self {
        self.props = self.props.with_app_id(app_id.into());
        self
    }

    /// Defaults to `None`.
    #[must_use]
    pub fn correlation_id(mut self, correlation_id: message::Id) -> Self {
        self.props = self
            .props
            .with_correlation_id(correlation_id.to_string().into());
        self
    }

    /// Defaults to `None`.
    #[must_use]
    pub fn headers<T>(mut self, headers: T) -> Self
    where
        T: Into<FieldTable>,
    {
        self.props = self.props.with_headers(headers.into());
        self
    }

    /// Defaults to a randomly generated [`message::Id`].
    #[must_use]
    pub fn message_id(mut self, message_id: message::Id) -> Self {
        self.props = self.props.with_message_id(message_id.to_string().into());
        self
    }

    /// Defaults to `None`.
    #[must_use]
    pub fn reply_to<T>(mut self, reply_to: T) -> Self
    where
        T: Into<ShortString>,
    {
        self.props = self.props.with_reply_to(reply_to.into());
        self
    }

    /// Defaults to [`Utc::now().timestamp()`].
    #[must_use]
    pub fn timestamp(mut self, timestamp: u64) -> Self {
        self.props = self.props.with_timestamp(timestamp);
        self
    }

    /// Set message properties.
    #[must_use]
    pub fn props<F>(mut self, f: F) -> Self
    where
        F: FnOnce(BasicProperties) -> BasicProperties,
    {
        self.props = f(self.props.clone());
        self
    }

    /// # Errors
    pub async fn publish<P>(self, payload: P) -> Result<PublisherConfirm, PublishError>
    where
        P: Serialize,
    {
        let chan = self
            .client
            .get_channel()
            .await
            .map_err(|err| PublishError(err.into()))?;
        let payload = message::Payload(payload)
            .serialize()
            .map_err(|err| PublishError(err.into()))?;
        chan.basic_publish(
            self.exchange_name,
            self.routing_key,
            self.opts,
            &payload,
            self.props,
        )
        .await
        .map_err(|err| PublishError(err.into()))
    }
}

/// Errors that can occur while publishing a message.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct PublishError(#[from] anyhow::Error);
