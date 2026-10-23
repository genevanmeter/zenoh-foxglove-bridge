//! Publishes Protobuf messages without a descriptor queryable.
//!
//! The bridge must be started with the descriptor generated from `examples/reading.proto`:
//! `protoc --descriptor_set_out=descriptor.bin --proto_path=examples examples/reading.proto`
//! `cargo run --bin zenoh-foxglove-bridge -- --descriptor-file descriptor.bin`
use prost::Message;
use zenoh_plugin_foxglove::Result;

#[derive(Clone, PartialEq, Message)]
struct Reading {
    #[prost(double, tag = "1")]
    value: f64,
    #[prost(string, tag = "2")]
    label: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = match std::env::args().nth(1) {
        Some(path) => zenoh::Config::from_file(path)?,
        None => zenoh::Config::default(),
    };
    let session = zenoh::open(config).await?;
    println!(
        "Publishing demo.Reading on proto_topic and proto_topic_2 every 100 ms (no descriptor queryable)"
    );
    let encoding = zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("demo.Reading");
    let mut ticks = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut value = 0.0_f64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = ticks.tick() => {
                for (topic, offset) in [("proto_topic", 0.0), ("proto_topic_2", 1.0)] {
                    let message = Reading {
                        value: (value + offset).sin(),
                        label: topic.into(),
                    };
                    session
                        .put(topic, message.encode_to_vec())
                        .encoding(encoding.clone())
                        .await?;
                }
                value += 0.1;
            }
        }
    }
    session.close().await?;
    Ok(())
}
