//! Self-contained publisher for proto_topic and proto_topic_2, including descriptors.
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
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
    let descriptor = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("reading.proto".into()),
            package: Some("demo".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Reading".into()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("value".into()),
                        number: Some(1),
                        label: Some(1),
                        r#type: Some(1),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("label".into()),
                        number: Some(2),
                        label: Some(1),
                        r#type: Some(9),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    let mut query_tasks = tokio::task::JoinSet::new();
    for topic in ["proto_topic", "proto_topic_2"] {
        let queryable = session
            .declare_queryable(format!("{topic}/protobuf_descriptor"))
            .await?;
        let descriptor = descriptor.clone();
        query_tasks.spawn(async move {
            while let Ok(query) = queryable.recv_async().await {
                if let Err(error) = query
                    .reply(query.key_expr(), descriptor.clone())
                    .encoding(
                        zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("demo.Reading"),
                    )
                    .await
                {
                    eprintln!("descriptor reply failed: {error}");
                }
            }
        });
    }
    println!("Publishing demo.Reading on proto_topic and proto_topic_2 every 100 ms");
    let mut ticks = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut value = 0.0_f64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = ticks.tick() => {
                for (topic, offset) in [("proto_topic", 0.0), ("proto_topic_2", 1.0)] {
                    session.put(topic, Reading { value: (value + offset).sin(), label: topic.into() }.encode_to_vec())
                        .encoding(zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("demo.Reading")).await?;
                }
                value += 0.1;
            }
        }
    }
    query_tasks.abort_all();
    session.close().await?;
    Ok(())
}
