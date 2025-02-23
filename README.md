# hare

`hare` is a `RabbitMQ` client that focuses on providing an ergonomic, streamlined, easy to setup and production ready `RabbitMQ` experience.

This repo is still under construction.

## Features
- Connection pooling using `deadpool`
- Easy `consumer` concurrency
- Consume any data in `consumers` using the `extractor pattern`

## Quickstart

```rust
/// Some shared state between `consumers`. Great
/// for passing database clients or the application config.
struct State {
    pub db: SomeClient,
}

/// The message payload to be extracted from the incoming message.
#[derive(serde::Deserialize)]
struct IncomingMsgPayload {
    pub foo: String
}

/// Consumes messages.
async fn consumer_handler(
    Json(event): Json<IncomingMsgPayload>, // This example uses the JSON extractor, but extractors can be used to deserialize all kinds of data.
    State(state): State,
) -> Result<(), anyhow::Error> {
    // Do something...
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
        .auto_delete(true)
        .declare()
        .await?;

    // Declare a `queue` using the queue declare builder.
    let queue = rabbitmq_client
        .queue_declare_builder("example_queue")
        .auto_delete(true)
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

    // Run the client and wait for all consumers to be finished.
    rabbitmq_client.run().await?;
}

```
