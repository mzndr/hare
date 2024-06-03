//! [`FromDeliveryData`] extractor implementations.
use std::ops::{Deref, DerefMut};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::{message, Client};

/// Extractor trait for [`Delivery`] objects. Extract data from a [`Delivery`] for
/// use in consumers. See [`crate::consumer_handler`].
#[async_trait]
pub(super) trait FromDeliveryData<S>: Sized
where
    S: Clone + Send + Sync + 'static,
{
    /// Extract Self from a [`Delivery`].
    async fn from_delivery_data(
        client: &Client<S>,
        delivery: &Delivery,
    ) -> Result<Self, anyhow::Error>;
}

#[async_trait]
impl<S, T, E> FromDeliveryData<S> for Result<T, E>
where
    S: Clone + Send + Sync + 'static,
    T: FromDeliveryData<S>,
    E: From<anyhow::Error>,
{
    async fn from_delivery_data(
        client: &Client<S>,
        delivery: &Delivery,
    ) -> Result<Self, anyhow::Error> {
        Ok(T::from_delivery_data(client, delivery)
            .await
            .map_err(E::from))
    }
}

#[async_trait]
impl<S, T> FromDeliveryData<S> for Option<T>
where
    S: Clone + Send + Sync + 'static,
    T: FromDeliveryData<S>,
{
    async fn from_delivery_data(
        client: &Client<S>,
        delivery: &Delivery,
    ) -> Result<Self, anyhow::Error> {
        Ok(T::from_delivery_data(client, delivery).await.ok())
    }
}

#[async_trait]
impl<S> FromDeliveryData<S> for Client<S>
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(client: &Client<S>, _: &Delivery) -> Result<Self, anyhow::Error> {
        Ok(client.clone())
    }
}

/// Get `Self` from a reference of `T`.
pub trait FromRef<T> {
    /// Get `Self` from a reference of `T`.
    fn from_ref(value: &T) -> Self;
}

impl<T> FromRef<T> for T
where
    T: Clone,
{
    fn from_ref(value: &T) -> Self {
        value.clone()
    }
}

/// State extractor.
#[derive(Debug, Clone)]
pub struct State<S>(pub S);

impl<S> Deref for State<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S> DerefMut for State<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[async_trait]
impl<InnerS, OuterS> FromDeliveryData<InnerS> for State<OuterS>
where
    InnerS: Clone + Send + Sync + 'static,
    OuterS: FromRef<InnerS>,
{
    async fn from_delivery_data(
        client: &Client<InnerS>,
        _: &Delivery,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self(OuterS::from_ref(&client.state)))
    }
}

/// Arc the delivery for efficent cloning.
pub type Delivery = Arc<lapin::message::Delivery>;

#[async_trait]
impl<S> FromDeliveryData<S> for Delivery
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        Ok(delivery.clone())
    }
}

/// `AppId` extractor.
#[derive(Debug, Clone)]
pub struct AppId<T>(pub T);

impl<T> Deref for AppId<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for AppId<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[async_trait]
impl<S> FromDeliveryData<S> for AppId<String>
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        let app_id_short_str = delivery
            .properties
            .app_id()
            .clone()
            .ok_or(anyhow::format_err!("app ID is missing"))?;
        let app_id = app_id_short_str.to_string();
        Ok(Self(app_id))
    }
}

/// `MessageId` extractor.
#[derive(Debug, Clone)]
pub struct MessageId<T>(pub T);

impl<T> Deref for MessageId<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for MessageId<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[async_trait]
impl<S> FromDeliveryData<S> for MessageId<String>
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        let msg_id_short_str = delivery
            .properties
            .message_id()
            .clone()
            .ok_or(anyhow::format_err!("message ID is missing"))?;
        let msg_id = msg_id_short_str.to_string();
        Ok(Self(msg_id))
    }
}

#[async_trait]
impl<S> FromDeliveryData<S> for MessageId<Uuid>
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        let msg_id_short_str = delivery
            .properties
            .message_id()
            .clone()
            .ok_or(anyhow::format_err!("message ID is missing"))?;
        let msg_id = Uuid::from_str(&msg_id_short_str.to_string())
            .map_err(|err| anyhow::format_err!("message ID not parseable: {err}"))?;
        Ok(Self(msg_id))
    }
}

#[async_trait]
impl<S> FromDeliveryData<S> for message::Id
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        let msg_id_short_str = delivery
            .properties
            .message_id()
            .clone()
            .ok_or(anyhow::format_err!("message ID is missing"))?;
        Self::from_str(&msg_id_short_str.to_string())
            .map_err(|err| anyhow::format_err!("message ID not parseable: {err}"))
    }
}

#[async_trait]
impl<S> FromDeliveryData<S> for Vec<u8>
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        Ok(delivery.data.clone())
    }
}

#[async_trait]
impl<S> FromDeliveryData<S> for Bytes
where
    S: Clone + Send + Sync + 'static,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        Ok(Self::copy_from_slice(&delivery.data))
    }
}

/// Json extractor.
#[derive(Debug, Clone)]
pub struct Json<T>(pub T);

impl<T> Deref for Json<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Json<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[async_trait]
impl<S, T> FromDeliveryData<S> for Json<T>
where
    S: Clone + Send + Sync + 'static,
    T: DeserializeOwned,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        let payload = serde_json::from_slice(&delivery.data)
            .map_err(|err| anyhow::format_err!("message payload not deserializable: {err}"))?;
        Ok(Self(payload))
    }
}

/// Protobuf extractor.
#[derive(Debug, Clone)]
pub struct Protobuf<T>(pub T);

impl<T> Deref for Protobuf<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for Protobuf<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[async_trait]
impl<S, T> FromDeliveryData<S> for Protobuf<T>
where
    S: Clone + Send + Sync + 'static,
    T: prost::Message + Default,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        let payload = T::decode(delivery.data.as_slice())
            .map_err(|err| anyhow::format_err!("message payload not decodeable: {err}"))?;
        Ok(Self(payload))
    }
}

#[async_trait]
impl<S, T> FromDeliveryData<S> for message::Payload<T>
where
    S: Clone + Send + Sync + 'static,
    T: DeserializeOwned,
{
    async fn from_delivery_data(_: &Client<S>, delivery: &Delivery) -> Result<Self, anyhow::Error> {
        Self::deserialize(delivery.data.as_slice())
            .map_err(|err| anyhow::format_err!("message payload not deserialize: {err}"))
    }
}
