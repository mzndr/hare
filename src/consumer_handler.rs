//! [`ConsumerHandler`] trait definition and implementation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use lapin::message::Delivery;

use crate::{
    Client, ConsumerHandlerError, ConsumerHandlerResult, FromDeliveryData,
    IntoConsumerHandlerResult,
};

/// `ConsumerHandlers` are functions that can get bound on `amqp queues` to handle the consumption of messages (see [`crate::basic_consume_builder::BasicConsumeBuilder`]).
/// A variety of extractors (see [`crate::amqp::consumer_extract::FromDeliveryData`]) can be used to extract different data from a
/// message or to provide shared state to the consumers.
///
/// `ConsumerHandler` holds two generic parameters, where `S` is the [`Client`] state, and `T` are
/// the parameters (extractors) passed to the `consumer`. `ConsumerHandler` is implemented for a range of tuples `T`, where every
/// member of `T` implements [`FromDeliveryData`] to provide flexibility when defining `ConsumerHandlers`, thus enabling zero
/// to eight extractors to be used in a `consumer` definition:
///
/// ```rust,ignore
///    impl<S, F, Fut, Res> ConsumerHandler<S, ()> for F where
///        F: FnOnce() -> Fut + Clone + Send + 'static,
///        // ...
///
///    impl<S, F, Fut, Res, P1> ConsumerHandler<S, (P1,)> for F where
///        F: FnOnce(P1) -> Fut + Clone + Send + 'static,
///        P1: FromDeliveryData<S> + Send,
///        // ...
///
///    impl<S, F, Fut, Res, P1, P2> ConsumerHandler<S, (P1, P2, /*...*/, P8)> for F
///    where
///        F: FnOnce(P1, P2, /* ... */, P8) -> Fut + Clone + Send + 'static,
///        P1: FromDeliveryData<S> + Send,
///        P2: FromDeliveryData<S> + Send,
///        // ...
///        P8: FromDeliveryData<S> + Send,
///
/// ```
pub trait ConsumerHandler<S, T>: Clone + Send + 'static
where
    S: Clone + Send + Sync + 'static,
{
    /// Functions implementing `ConsumerHandler` have to yield [`Future`]s resulting
    /// in [`ConsumerHandlerResult`]s.
    type Future: Future<Output = ConsumerHandlerResult> + Send + 'static;

    /// Call executes the consumer handler for a [`Delivery`].
    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future;
}

impl<S, F, Fut, Res> ConsumerHandler<S, ()> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, _: Client<S>, _: Arc<Delivery>) -> Self::Future {
        Box::pin(async move { self().await.into_consumer_handler_result() })
    }
}

impl<S, F, Fut, Res, P1> ConsumerHandler<S, (P1,)> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            self(p1).await.into_consumer_handler_result()
        })
    }
}

impl<S, F, Fut, Res, P1, P2> ConsumerHandler<S, (P1, P2)> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1, P2) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
    P2: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            let p2 = P2::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P2>().to_string(), err)
                })?;
            self(p1, p2).await.into_consumer_handler_result()
        })
    }
}

impl<S, F, Fut, Res, P1, P2, P3> ConsumerHandler<S, (P1, P2, P3)> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1, P2, P3) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
    P2: FromDeliveryData<S> + Send,
    P3: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            let p2 = P2::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P2>().to_string(), err)
                })?;
            let p3 = P3::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P3>().to_string(), err)
                })?;
            self(p1, p2, p3).await.into_consumer_handler_result()
        })
    }
}

impl<S, F, Fut, Res, P1, P2, P3, P4> ConsumerHandler<S, (P1, P2, P3, P4)> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1, P2, P3, P4) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
    P2: FromDeliveryData<S> + Send,
    P3: FromDeliveryData<S> + Send,
    P4: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            let p2 = P2::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P2>().to_string(), err)
                })?;
            let p3 = P3::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P3>().to_string(), err)
                })?;
            let p4 = P4::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P4>().to_string(), err)
                })?;
            self(p1, p2, p3, p4).await.into_consumer_handler_result()
        })
    }
}

impl<S, F, Fut, Res, P1, P2, P3, P4, P5> ConsumerHandler<S, (P1, P2, P3, P4, P5)> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1, P2, P3, P4, P5) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
    P2: FromDeliveryData<S> + Send,
    P3: FromDeliveryData<S> + Send,
    P4: FromDeliveryData<S> + Send,
    P5: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            let p2 = P2::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P2>().to_string(), err)
                })?;
            let p3 = P3::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P3>().to_string(), err)
                })?;
            let p4 = P4::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P4>().to_string(), err)
                })?;
            let p5 = P5::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P5>().to_string(), err)
                })?;
            self(p1, p2, p3, p4, p5)
                .await
                .into_consumer_handler_result()
        })
    }
}

impl<S, F, Fut, Res, P1, P2, P3, P4, P5, P6> ConsumerHandler<S, (P1, P2, P3, P4, P5, P6)> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1, P2, P3, P4, P5, P6) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
    P2: FromDeliveryData<S> + Send,
    P3: FromDeliveryData<S> + Send,
    P4: FromDeliveryData<S> + Send,
    P5: FromDeliveryData<S> + Send,
    P6: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            let p2 = P2::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P2>().to_string(), err)
                })?;
            let p3 = P3::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P3>().to_string(), err)
                })?;
            let p4 = P4::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P4>().to_string(), err)
                })?;
            let p5 = P5::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P5>().to_string(), err)
                })?;
            let p6 = P6::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P6>().to_string(), err)
                })?;
            self(p1, p2, p3, p4, p5, p6)
                .await
                .into_consumer_handler_result()
        })
    }
}

impl<S, F, Fut, Res, P1, P2, P3, P4, P5, P6, P7> ConsumerHandler<S, (P1, P2, P3, P4, P5, P6, P7)>
    for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1, P2, P3, P4, P5, P6, P7) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
    P2: FromDeliveryData<S> + Send,
    P3: FromDeliveryData<S> + Send,
    P4: FromDeliveryData<S> + Send,
    P5: FromDeliveryData<S> + Send,
    P6: FromDeliveryData<S> + Send,
    P7: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            let p2 = P2::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P2>().to_string(), err)
                })?;
            let p3 = P3::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P3>().to_string(), err)
                })?;
            let p4 = P4::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P4>().to_string(), err)
                })?;
            let p5 = P5::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P5>().to_string(), err)
                })?;
            let p6 = P6::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P6>().to_string(), err)
                })?;
            let p7 = P7::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P7>().to_string(), err)
                })?;
            self(p1, p2, p3, p4, p5, p6, p7)
                .await
                .into_consumer_handler_result()
        })
    }
}

impl<S, F, Fut, Res, P1, P2, P3, P4, P5, P6, P7, P8>
    ConsumerHandler<S, (P1, P2, P3, P4, P5, P6, P7, P8)> for F
where
    S: Clone + Send + Sync + 'static,
    F: FnOnce(P1, P2, P3, P4, P5, P6, P7, P8) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Res> + Send,
    Res: IntoConsumerHandlerResult,
    P1: FromDeliveryData<S> + Send,
    P2: FromDeliveryData<S> + Send,
    P3: FromDeliveryData<S> + Send,
    P4: FromDeliveryData<S> + Send,
    P5: FromDeliveryData<S> + Send,
    P6: FromDeliveryData<S> + Send,
    P7: FromDeliveryData<S> + Send,
    P8: FromDeliveryData<S> + Send,
{
    type Future = Pin<Box<dyn Future<Output = ConsumerHandlerResult> + Send>>;

    fn call(self, client: Client<S>, delivery: Arc<Delivery>) -> Self::Future {
        Box::pin(async move {
            let p1 = P1::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P1>().to_string(), err)
                })?;
            let p2 = P2::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P2>().to_string(), err)
                })?;
            let p3 = P3::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P3>().to_string(), err)
                })?;
            let p4 = P4::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P4>().to_string(), err)
                })?;
            let p5 = P5::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P5>().to_string(), err)
                })?;
            let p6 = P6::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P6>().to_string(), err)
                })?;
            let p7 = P7::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P7>().to_string(), err)
                })?;
            let p8 = P8::from_delivery_data(&client, &delivery)
                .await
                .map_err(|err| {
                    ConsumerHandlerError::Extractor(std::any::type_name::<P8>().to_string(), err)
                })?;
            self(p1, p2, p3, p4, p5, p6, p7, p8)
                .await
                .into_consumer_handler_result()
        })
    }
}
