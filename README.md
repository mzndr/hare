# hare

`hare` is a `RabbitMQ` client that focuses on a streamlined and production ready experience.

This repo is still under construction.

## Features
- Connection pooling using `deadpool`
- Easy consumer concurrency
- Consume any data `consumers` using the `extractor pattern`

## Quickstart

```rust
/// Some shared state between `consumers`. Great
/// for passing database clients or the application config.
struct State {
    pub db: SomeClient,
}

/// The message payload to be extracted from the incomming message. Assumed to be deserializable `JSON` data,
/// but can be any format (e.g. `Protobuffs`) supported by `extractors`.
#[derive(serde::Deserialize)]
struct IncommingMsgPaylaod {
    pub foo: String
}

/// Consumes messages.
async fn consumer_handler(
    Json(event): Json<request::Event>, // Consumer parameters are very flexible due to the extractor pattern.
    State: State,                      //
) -> Result<(), anyhow::Error> {
    // Do something..
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create the client.
    let rabbitmq_client = hare::Client::new(
        "amqp://guest:guest@localhost:5672",
        hare::lapin::ConnectionProperties::default(),
        "example app",
        State::new(&db),
    )
    .await?;

    let exchange_name = "example_exchange";

    // Declare an `exchange` using the exchange declare builder.
    rabbitmq_client.exchange_declare_builder(exchange_name, hare::lapin::ExchangeKind::Topic)
        .auto_delete(true) // Delete the exchange when no consumers are present anymore.
        .declare()
        .await?;

    // Declare a `queue` using the queue declare builder.
    let queue = rabbitmq_client
        .queue_declare_builder("example_queue")
        .auto_delete(true) // Delete the queue when no exchange is present anymore.
        .declare()
        .await?;

    // Declare a `consumer` and set it to consume from the `queue`.
    rabbitmq_client
        .basic_consume_builder(&queue, "example_consumer")
        .consume(consumer_handler)
        .await?;

    // Bind the `queue` to the `exchange`.
    rabbitmq_client
        .queue_bind_builder(&queue, exchange_name, "example.routing_key")
        .bind()
        .await?;
}

```
