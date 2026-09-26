# Zenoh Foxglove bridge

A standalone Rust executable and a `zenohd` plugin that forward Zenoh messages to a Foxglove WebSocket server for Foxglove Studio and Lichtblick. Both use the Foxglove SDK (`foxglove.sdk.v1`) and advertise topics as messages arrive. Lichtblick 1.22.0 or newer is required; your 1.28.1 version supports this protocol. Defaults: `ws://127.0.0.1:8765`, Zenoh subscription `**` (including `proto_topic` and `proto_topic_2`).

## Build and run

```sh
cargo build --release --locked
# Standalone peer, using Zenoh's default discovery:
./target/release/zenoh-foxglove-bridge
# Or connect explicitly to an existing router:
./target/release/zenoh-foxglove-bridge --config config/bridge.json --zenoh-config config/client.json5
```

In Foxglove Studio or Lichtblick, open a **Foxglove WebSocket** connection to `ws://127.0.0.1:8765`. Add a Raw Messages panel to inspect either topic, or a Plot panel to plot numeric fields. Custom Protobuf messages appear by their field names; specialized 3D panels require compatible message schemas.

For remote viewers, set `bind` to the host's interface or `0.0.0.0`, then connect using that host's address. The server is read-only and provides no authentication or TLS; the default binding is loopback.

## zenohd plugin

The same build produces `target/release/libzenoh_plugin_foxglove.so` on Linux (`.dylib` on macOS, `.dll` on Windows). Use a **Zenoh 1.10.1** router. The installed router and plugin should be built with compatible Rust toolchains; Zenoh checks plugin compatibility on load.

```sh
zenohd -c config/zenohd.json5
```

Run this from the repository root, or change `plugins.foxglove.__path__` to the absolute library path. The example router listens at `tcp/127.0.0.1:7447`. Plugin settings are directly under `plugins.foxglove`; they have the same names as `config/bridge.json`. The plugin shares the router's runtime and shuts down its server and tasks when unloaded. Restart the plugin to apply bridge setting changes. Run either the standalone server or plugin on a given WebSocket port.

## Protobuf descriptor contract

For each Protobuf topic, the **publisher** exposes a Zenoh queryable on `<topic>/protobuf_descriptor`. For your reference topics these are:

- `proto_topic/protobuf_descriptor`
- `proto_topic_2/protobuf_descriptor`

Reply on that same key with the raw binary **`google.protobuf.FileDescriptorSet`**, including imported files. Do not send `.proto` source text, base64 text, or a single `FileDescriptorProto`. The bridge queries the descriptor, advertises a `protobuf` channel with the descriptor as its schema, and forwards the original message bytes. The SDK handles WebSocket schema base64 encoding. This follows [Foxglove's Protobuf schema format](https://docs.foxglove.dev/docs/getting-started/custom/custom-schema-encodings).

Generate a descriptor with:

```sh
protoc --include_imports --descriptor_set_out=descriptor.bin --proto_path=. your_message.proto
```

The bridge selects the Protobuf message name in this order:

1. An explicit `message_types` entry for the topic.
2. The data sample's `application/protobuf` encoding schema.
3. The descriptor reply's `application/protobuf` encoding schema.
4. Inference when there is exactly one top-level message in the root descriptor files (files not imported by another file).

Publish the fully qualified name as Zenoh encoding metadata to support automatic discovery even when the descriptor contains multiple message types:

```rust
.encoding(zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema("example.msg.basic.Basic"))
```

The encoding schema contains the message name; the queryable still returns the binary `FileDescriptorSet`. Empty encoding schemas fall back to inference. Names supplied through metadata must exist in the descriptor, just like configured names. For publishers without type metadata, or to override it, configure the actual type explicitly:

```json
{
  "message_types": {
    "proto_topic": "your.package.FirstMessage",
    "proto_topic_2": "your.package.SecondMessage"
  }
}
```

Configured types must exist in the descriptor; nested message names are supported. Set the Zenoh message encoding to `application/protobuf` when possible. Unlabelled byte payloads also trigger descriptor discovery, so existing publishers that omit encoding work. Descriptor keys are metadata and are not advertised as data channels.

Missing, malformed, or ambiguous descriptors generate a warning and are retried when subsequent messages arrive after `retry_interval_ms`. The first message is held while its descriptor is queried; bounded queues buffer further messages. Once established, a topic's encoding and schema are fixed until the bridge restarts. A topic must use one stable encoding and message type, with consistent descriptor responses from all publishers.

## JSON and resource limits

Zenoh `application/json` and `text/json` messages are forwarded as JSON channels with a permissive object schema. Invalid JSON and non-object JSON values are dropped. Other payloads require a Protobuf descriptor; arbitrary raw bytes, ROS/CDR, and text are not automatically converted.

`config/bridge.json` lists all settings. `key_expr` selects topics using a Zenoh key expression. Descriptor queries time out after `query_timeout_ms`. Each topic has an independent worker and `queue_capacity` pending messages, so a missing descriptor does not block other topics. Full topic queues drop new arrivals (logged at debug level). `max_topics` bounds the number of tracked topics; `max_message_bytes` limits payload and descriptor sizes. Channels remain registered until shutdown. These are live streams: there is no history replay, and messages sent before a viewer subscribes are not retained for it.

Zenoh timestamps are preserved when present; otherwise the bridge uses its receive time. Set `RUST_LOG=debug` for standalone diagnostics. The plugin uses the router's logging configuration.

## Self-contained demo and tests

```sh
# Terminal 1:
cargo run --bin zenoh-foxglove-bridge
# Terminal 2 (provides both topic queryables and their demo.Reading descriptors):
cargo run --example protobuf_publisher
# With the example router instead:
cargo run --example protobuf_publisher -- config/client.json5

cargo test --locked
```

The integration test uses an isolated Zenoh session and a real Foxglove SDK WebSocket client to check schema advertisements, descriptor retrieval, exact message bytes, timestamps, JSON forwarding, late descriptors, and shutdown.

To test the built executable and dynamically loaded plugin against an installed `zenohd` 1.10.1, run:

```sh
cargo build --locked --bins --lib --examples
python3 tests/runtime_smoke.py
```

This process-level smoke test starts isolated local routers, publishes both reference topics with their descriptor queryables, and verifies receipt through each WebSocket server.
