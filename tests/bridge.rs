use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use serde_json::{Value, json};
use std::time::Duration;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message as WsMessage, client::IntoClientRequest},
};
use zenoh_plugin_foxglove::{Bridge, BridgeConfig, schema::protobuf_schema};

fn descriptor(names: &[&str]) -> Vec<u8> {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("test.proto".into()),
            package: Some("test".into()),
            syntax: Some("proto3".into()),
            message_type: names
                .iter()
                .map(|name| DescriptorProto {
                    name: Some((*name).into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("value".into()),
                        number: Some(1),
                        label: Some(1),
                        r#type: Some(5),
                        ..Default::default()
                    }],
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

#[test]
fn descriptor_selection_and_errors() {
    assert_eq!(
        protobuf_schema(descriptor(&["Reading"]), None)
            .unwrap()
            .name,
        "test.Reading"
    );
    assert!(protobuf_schema(descriptor(&["A", "B"]), None).is_err());
    assert_eq!(
        protobuf_schema(descriptor(&["A", "B"]), Some(".test.B"))
            .unwrap()
            .name,
        "test.B"
    );
    assert!(protobuf_schema(descriptor(&["A"]), Some("test.Missing")).is_err());
    assert!(protobuf_schema(vec![], None).is_err());
    assert!(protobuf_schema(vec![255], None).is_err());
    let mut set = FileDescriptorSet::decode(descriptor(&["A"]).as_slice()).unwrap();
    set.file[0].dependency.push("missing.proto".into());
    assert!(
        protobuf_schema(set.encode_to_vec(), None)
            .unwrap_err()
            .to_string()
            .contains("missing import")
    );
    let imported = FileDescriptorProto {
        name: Some("missing.proto".into()),
        package: Some("dependency".into()),
        message_type: vec![DescriptorProto {
            name: Some("Imported".into()),
            ..Default::default()
        }],
        ..Default::default()
    };
    set.file.insert(0, imported);
    assert_eq!(
        protobuf_schema(set.encode_to_vec(), None).unwrap().name,
        "test.A"
    );
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
async fn next_json(socket: &mut Socket) -> Value {
    loop {
        match socket.next().await.unwrap().unwrap() {
            WsMessage::Text(text) => return serde_json::from_str(&text).unwrap(),
            WsMessage::Ping(data) => socket.send(WsMessage::Pong(data)).await.unwrap(),
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn websocket_roundtrip_and_late_descriptor() {
    roundtrip(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn encoding_schema_selects_message_from_multi_message_descriptor() {
    roundtrip(1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn configured_type_overrides_encoding_schema() {
    roundtrip(2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descriptor_reply_encoding_can_supply_message_type() {
    roundtrip(3).await;
}

async fn roundtrip(mode: u8) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut config = zenoh::Config::default();
        config.insert_json5("scouting/multicast/enabled", "false").unwrap();
        config.insert_json5("listen/endpoints", "[]").unwrap();
        let session = zenoh::open(config).await.unwrap();
        let bridge = Bridge::start(session.clone(), BridgeConfig { port: 0, query_timeout_ms: 100,
            retry_interval_ms: 100,
            message_types: if mode == 2 { [("proto_topic".into(), "test.Reading".into())].into() } else { Default::default() },
            ..Default::default() }).await.unwrap();
        let port = bridge.port();
        let mut request = format!("ws://127.0.0.1:{port}").into_client_request().unwrap();
        request.headers_mut().insert("Sec-WebSocket-Protocol", "foxglove.sdk.v1".parse().unwrap());
        let (mut socket, response) = connect_async(request).await.unwrap();
        assert_eq!(response.headers()["Sec-WebSocket-Protocol"], "foxglove.sdk.v1");
        assert_eq!(next_json(&mut socket).await["op"], "serverInfo");
        let bytes = if mode == 0 { descriptor(&["Reading"]) } else { descriptor(&["Other", "Reading"]) };
        let q = session.declare_queryable("proto_topic/protobuf_descriptor").await.unwrap();
        let copy = bytes.clone();
        let query_task = tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                let encoding = if mode == 3 {
                    zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("test.Reading")
                } else { zenoh::bytes::Encoding::APPLICATION_PROTOBUF };
                query.reply(query.key_expr(), copy.clone()).encoding(encoding).await.unwrap();
            }
        });
        // A missing descriptor must not prevent discovery of the healthy topic or JSON.
        session.put("proto_topic_2", vec![8u8, 42]).await.unwrap();
        let encoding = match mode {
            1 => zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("test.Reading"),
            2 => zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("test.Other"),
            _ => zenoh::bytes::Encoding::APPLICATION_PROTOBUF,
        };
        session.put("proto_topic", vec![8u8, 42]).encoding(encoding).await.unwrap();
        session.put("json_topic", r#"{"value":7}"#).encoding("application/json").await.unwrap();
        // Metadata keys must never be advertised as data channels.
        session.put("ignored/protobuf_descriptor", bytes.clone()).await.unwrap();
        let mut advertisements = std::collections::HashMap::new();
        while advertisements.len() < 2 {
            let message = next_json(&mut socket).await;
            if message["op"] == "advertise" {
                for channel in message["channels"].as_array().unwrap() {
                    advertisements.insert(channel["topic"].as_str().unwrap().to_owned(), channel.clone());
                }
            }
        }
        let proto = &advertisements["proto_topic"];
        assert_eq!(proto["encoding"], "protobuf");
        assert_eq!(proto["schemaName"], "test.Reading");
        assert_eq!(STANDARD.decode(proto["schema"].as_str().unwrap()).unwrap(), bytes);
        assert_eq!(advertisements["json_topic"]["encoding"], "json");
        socket.send(WsMessage::Text(json!({"op":"subscribe","subscriptions":[
            {"id":11,"channelId":proto["id"]}, {"id":12,"channelId":advertisements["json_topic"]["id"]}
        ]}).to_string().into())).await.unwrap();
        // Publish repeatedly until the server has processed the subscription.
        let publisher = session.clone();
        let timestamp = session.new_timestamp();
        let expected_time = timestamp.get_time().to_system_time().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
        let publish_task = tokio::spawn(async move {
            loop {
                publisher.put("proto_topic", vec![8u8, 99]).timestamp(timestamp).await.unwrap();
                publisher.put("json_topic", r#"{"value":9}"#).encoding("application/json").await.unwrap();
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        });
        let mut seen = std::collections::HashSet::new();
        while seen.len() < 2 {
            if let WsMessage::Binary(data) = socket.next().await.unwrap().unwrap() {
                assert_eq!(data[0], 1);
                let id = u32::from_le_bytes(data[1..5].try_into().unwrap());
                if id == 11 {
                    assert_eq!(&data[13..], &[8, 99]);
                    assert_eq!(u64::from_le_bytes(data[5..13].try_into().unwrap()), expected_time);
                } else { assert_eq!(id, 12); assert_eq!(&data[13..], br#"{"value":9}"#); }
                seen.insert(id);
            }
        }
        publish_task.abort();
        let q2 = session.declare_queryable("proto_topic_2/protobuf_descriptor").await.unwrap();
        let query_task2 = tokio::spawn(async move {
            while let Ok(query) = q2.recv_async().await { query.reply(query.key_expr(), bytes.clone()).encoding(zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("test.Reading")).await.unwrap(); }
        });
        tokio::time::sleep(Duration::from_millis(250)).await;
        session.put("proto_topic_2", vec![8u8, 55]).await.unwrap();
        loop {
            let message = next_json(&mut socket).await;
            if message["op"] == "advertise" && message["channels"].as_array().unwrap().iter().any(|c| c["topic"] == "proto_topic_2") { break; }
        }
        socket.close(None).await.unwrap();
        bridge.shutdown().await;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.unwrap();
        drop(listener);
        query_task.abort(); query_task2.abort();
        session.close().await.unwrap();
    }).await.expect("bridge integration test timed out");
}
