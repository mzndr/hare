//! Hare, provide a streamlined and production ready `RabbitMQ` experience in Rust.
#![forbid(unsafe_code)]
#![deny(nonstandard_style)]
#![warn(
    missing_docs,
    clippy::missing_docs_in_private_items,
    clippy::pedantic,
    clippy::unwrap_used
)]
#![allow(clippy::unused_async)]

mod builder_arguments;
mod consumer_handler;
mod consumer_handler_result;
pub mod basic_consume_builder;
pub mod basic_publish_builder;
pub mod channel_pool;
pub mod client;
pub mod consumer_extract;
pub mod dlx;
pub mod exchange_declare_builder;
pub mod message;
pub mod queue;
pub mod queue_bind_builder;
pub mod queue_declare_builder;
pub mod queue_delete_builder;
pub mod queue_purge_builder;
pub mod queue_unbind_builder;
pub mod rpc_builder;
pub use builder_arguments::BuilderArgs;
pub use channel_pool::Channel;
pub use client::Client;
pub use consumer_handler::ConsumerHandler;
pub use lapin;
pub use queue::Queue;
use basic_consume_builder::BasicConsumeBuilder;
use basic_publish_builder::BasicPublishBuilder;
use channel_pool::ChannelPool;
use consumer_extract::FromDeliveryData;
use consumer_handler_result::{ ConsumerHandlerError, ConsumerHandlerResult, IntoConsumerHandlerResult, };
use exchange_declare_builder::ExchangeDeclareBuilder;
use queue::QueueName;
use queue_bind_builder::QueueBindBuilder;
use queue_declare_builder::QueueDeclareBuilder;
use queue_delete_builder::QueueDeleteBuilder;
use queue_purge_builder::QueuePurgeBuilder;
use queue_unbind_builder::QueueUnbindBuilder;
use rpc_builder::RpcBuilder;

pub mod util;

/// Prelude
pub mod prelude {
    pub use crate::BuilderArgs as _;
    pub use crate::util::OnError as _;
}
