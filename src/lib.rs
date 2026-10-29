//! Zenoh to Foxglove WebSocket bridge shared by the executable and zenohd plugin.
mod plugin;
pub mod schema;

use foxglove::{
    ChannelBuilder, Context, PartialMetadata, RawChannel, WebSocketServer, WebSocketServerHandle,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use zenoh::{
    Session,
    sample::{Sample, SampleKind},
};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BridgeConfig {
    pub bind: String,
    pub port: u16,
    pub key_expr: String,
    pub message_types: HashMap<String, String>,
    /// Path to a binary google.protobuf.FileDescriptorSet shared by Protobuf topics.
    pub descriptor_file: Option<String>,
    pub query_timeout_ms: u64,
    pub retry_interval_ms: u64,
    pub queue_capacity: usize,
    pub max_topics: usize,
    pub max_message_bytes: usize,
}

impl Default for BridgeConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".into(),
            port: 8765,
            key_expr: "**".into(),
            message_types: HashMap::new(),
            descriptor_file: None,
            query_timeout_ms: 2000,
            retry_interval_ms: 5000,
            queue_capacity: 32,
            max_topics: 1024,
            max_message_bytes: 16 * 1024 * 1024,
        }
    }
}

impl BridgeConfig {
    fn validate(&self) -> Result<()> {
        zenoh::key_expr::KeyExpr::try_from(self.key_expr.as_str())?;
        if self.query_timeout_ms == 0
            || self.retry_interval_ms == 0
            || self.queue_capacity == 0
            || self.max_topics == 0
            || self.max_message_bytes == 0
        {
            return Err("timeouts and resource limits must be positive".into());
        }
        Ok(())
    }
}

pub struct Bridge {
    task: Option<JoinHandle<()>>,
    server: Option<WebSocketServerHandle>,
}

impl Bridge {
    pub async fn start(session: Session, config: BridgeConfig) -> Result<Self> {
        config.validate()?;
        let descriptor = Arc::new(load_descriptor_file(&config)?);
        let subscriber = session
            .declare_subscriber(config.key_expr.clone())
            .with(zenoh::handlers::FifoChannel::new(config.queue_capacity))
            .await?;
        let context = Context::new();
        let server = WebSocketServer::new()
            .name("Zenoh Foxglove Bridge")
            .bind(&config.bind, config.port)
            .context(&context)
            .tokio_runtime(&tokio::runtime::Handle::current())
            .start()
            .await?;
        tracing::info!(port = server.port(), bind = %config.bind, "Foxglove WebSocket listening");
        let task = tokio::spawn(async move {
            let session = Arc::new(session);
            let config = Arc::new(config);
            let mut topics: HashMap<String, mpsc::Sender<(Sample, SystemTime)>> = HashMap::new();
            let mut workers = JoinSet::new();
            while let Ok(sample) = subscriber.recv_async().await {
                let topic = sample.key_expr().as_str();
                if sample.kind() != SampleKind::Put || topic.ends_with("/protobuf_descriptor") {
                    continue;
                }
                if sample.payload().len() > config.max_message_bytes {
                    tracing::warn!(%topic, "dropping oversized message");
                    continue;
                }
                if !topics.contains_key(topic) {
                    if topics.len() >= config.max_topics {
                        tracing::warn!(%topic, "topic limit reached");
                        continue;
                    }
                    let (tx, rx) = mpsc::channel(config.queue_capacity);
                    topics.insert(topic.to_owned(), tx);
                    workers.spawn(topic_worker(
                        topic.to_owned(),
                        rx,
                        session.clone(),
                        config.clone(),
                        descriptor.clone(),
                        context.clone(),
                    ));
                }
                if let Err(error) = topics[topic].try_send((sample.clone(), SystemTime::now())) {
                    tracing::debug!(%topic, %error, "dropping message from full or closed topic queue");
                }
            }
            // Dropping JoinSet cancels all workers and their outstanding descriptor queries.
            tracing::info!("Zenoh subscriber closed");
        });
        Ok(Self {
            task: Some(task),
            server: Some(server),
        })
    }

    pub fn port(&self) -> u16 {
        self.server.as_ref().expect("running server").port()
    }

    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(server) = self.server.take() {
            server.stop().wait().await;
        }
    }
}

fn load_descriptor_file(config: &BridgeConfig) -> Result<Option<Vec<u8>>> {
    let Some(path) = &config.descriptor_file else {
        return Ok(None);
    };
    let bytes = std::fs::read(path)
        .map_err(|error| format!("failed to read descriptor file at {path}: {error}"))?;
    if bytes.len() > config.max_message_bytes {
        return Err(format!("descriptor file at {path} exceeds max_message_bytes").into());
    }
    Ok(Some(bytes))
}

impl Drop for Bridge {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        if let Some(server) = self.server.take() {
            server.stop().detach_all();
        }
    }
}

async fn topic_worker(
    topic: String,
    mut rx: mpsc::Receiver<(Sample, SystemTime)>,
    session: Arc<Session>,
    config: Arc<BridgeConfig>,
    descriptor: Arc<Option<Vec<u8>>>,
    context: Arc<Context>,
) {
    let mut channel: Option<CreatedChannel> = None;
    let mut retry_at = tokio::time::Instant::now();
    while let Some((sample, received)) = rx.recv().await {
        if channel.is_none() {
            if tokio::time::Instant::now() < retry_at {
                continue;
            }
            match create_channel(&topic, &sample, &session, &config, &descriptor, &context).await {
                Ok(created) => {
                    tracing::info!(%topic, schema = ?created.channel.schema(), "advertising channel");
                    channel = Some(created);
                }
                Err(error) => {
                    tracing::warn!(%topic, %error, "channel discovery failed; retrying on later messages");
                    retry_at = tokio::time::Instant::now()
                        + Duration::from_millis(config.retry_interval_ms);
                    continue;
                }
            }
        }
        let created = channel.as_ref().unwrap();
        let payload = sample.payload().to_bytes();
        let payload = match created.format {
            PayloadFormat::Original => payload.into_owned(),
            PayloadFormat::JsonObject => {
                if !serde_json::from_slice::<serde_json::Value>(&payload)
                    .is_ok_and(|v| v.is_object())
                {
                    tracing::warn!(%topic, "dropping invalid JSON object");
                    continue;
                }
                payload.into_owned()
            }
            PayloadFormat::RawBytes => serde_json::to_vec(&serde_json::json!({
                "encoding": sample.encoding().to_string(),
                "data": payload.as_ref(),
            }))
            .expect("raw byte envelope is always valid JSON"),
            PayloadFormat::Text => match std::str::from_utf8(&payload) {
                Ok(text) => serde_json::to_vec(&serde_json::json!({ "data": text }))
                    .expect("UTF-8 text envelope is always valid JSON"),
                Err(error) => {
                    tracing::warn!(%topic, %error, "dropping invalid UTF-8 text");
                    continue;
                }
            },
        };
        let time = sample
            .timestamp()
            .map(|t| t.get_time().to_system_time())
            .unwrap_or(received);
        created
            .channel
            .log_with_meta(&payload, PartialMetadata::with_log_time(time));
    }
}

#[derive(Clone, Copy)]
enum PayloadFormat {
    Original,
    JsonObject,
    RawBytes,
    Text,
}

struct CreatedChannel {
    channel: Arc<RawChannel>,
    format: PayloadFormat,
}

async fn create_channel(
    topic: &str,
    sample: &Sample,
    session: &Session,
    config: &BridgeConfig,
    descriptor: &Option<Vec<u8>>,
    context: &Arc<Context>,
) -> Result<CreatedChannel> {
    let encoding = sample.encoding().to_string();
    let mime = encoding.split(';').next().unwrap_or(&encoding);
    let builder = ChannelBuilder::new(topic).context(context);
    if matches!(mime, "application/json" | "text/json") && !config.message_types.contains_key(topic)
    {
        return Ok(CreatedChannel {
            channel: builder
                .message_encoding("json")
                .schema(foxglove::Schema::new(
                    "json",
                    "jsonschema",
                    br#"{"type":"object","additionalProperties":true}"#.to_vec(),
                ))
                .build_raw()?,
            format: PayloadFormat::JsonObject,
        });
    }
    if mime == "text/plain" && !config.message_types.contains_key(topic) {
        return Ok(CreatedChannel {
            channel: builder
                .message_encoding("json")
                .schema(foxglove::Schema::new(
                    "text",
                    "jsonschema",
                    br#"{"type":"object","properties":{"data":{"type":"string"}},"required":["data"],"additionalProperties":false}"#.to_vec(),
                ))
                .build_raw()?,
            format: PayloadFormat::Text,
        });
    }
    let is_protobuf = config.message_types.contains_key(topic)
        || sample.encoding().id() == zenoh::bytes::Encoding::APPLICATION_PROTOBUF.id();
    if !is_protobuf {
        return Ok(CreatedChannel {
            channel: builder
                .message_encoding("json")
                .schema(foxglove::Schema::new(
                    format!("{topic}.RawBytes"),
                    "jsonschema",
                    br#"{"type":"object","properties":{"encoding":{"type":"string"},"data":{"type":"array","items":{"type":"integer","minimum":0,"maximum":255}}},"required":["encoding","data"],"additionalProperties":false}"#.to_vec(),
                ))
                .build_raw()?,
            format: PayloadFormat::RawBytes,
        });
    }
    let message_type = match config.message_types.get(topic) {
        Some(name) => Some(name.as_str()),
        None => protobuf_type(sample.encoding())?,
    };
    if let Some(bytes) = descriptor {
        let schema = schema::protobuf_schema(bytes.clone(), message_type)?;
        return Ok(CreatedChannel {
            channel: builder
                .message_encoding("protobuf")
                .schema(schema)
                .build_raw()?,
            format: PayloadFormat::Original,
        });
    }
    // Explicit Protobuf topics without a configured descriptor discover it from the publisher.
    let key = format!("{topic}/protobuf_descriptor");
    let replies = session
        .get(&key)
        .target(zenoh::query::QueryTarget::All)
        .consolidation(zenoh::query::ConsolidationMode::None)
        .timeout(Duration::from_millis(config.query_timeout_ms))
        .await?;
    let mut last_error = format!("no descriptor reply from {key}");
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(reply) if reply.key_expr().as_str() == key => {
                if reply.payload().len() > config.max_message_bytes {
                    last_error = "descriptor exceeds max_message_bytes".into();
                    continue;
                }
                match schema::protobuf_schema(
                    reply.payload().to_bytes().into_owned(),
                    match message_type {
                        Some(name) => Some(name),
                        None => protobuf_type(reply.encoding())?,
                    },
                ) {
                    Ok(schema) => {
                        return Ok(CreatedChannel {
                            channel: builder
                                .message_encoding("protobuf")
                                .schema(schema)
                                .build_raw()?,
                            format: PayloadFormat::Original,
                        });
                    }
                    Err(error) => last_error = error.to_string(),
                }
            }
            Err(error) => last_error = format!("descriptor query error: {error:?}"),
            _ => {}
        }
    }
    Err(last_error.into())
}

// Zenoh's schema bytes carry the fully qualified message name, not the descriptor.
fn protobuf_type(encoding: &zenoh::bytes::Encoding) -> Result<Option<&str>> {
    if encoding.id() != zenoh::bytes::Encoding::APPLICATION_PROTOBUF.id() {
        return Ok(None);
    }
    encoding
        .schema()
        .map(|schema| {
            std::str::from_utf8(schema.as_ref())
                .map(str::trim)
                .map_err(|error| {
                    format!("Protobuf encoding schema must be a UTF-8 message name: {error}").into()
                })
        })
        .transpose()
        .map(|name| name.filter(|name| !name.is_empty()))
}
