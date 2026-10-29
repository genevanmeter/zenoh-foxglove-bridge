//! Publishes a procedural RGB image using Foxglove's JSON RawImage schema.
use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use zenoh_plugin_foxglove::Result;

const TOPIC: &str = "demo/foxglove/raw_image";

#[tokio::main]
async fn main() -> Result<()> {
    let config = match std::env::args().nth(1) {
        Some(path) => zenoh::Config::from_file(path)?,
        None => zenoh::Config::default(),
    };
    let session = zenoh::open(config).await?;
    let mut ticks = tokio::time::interval(Duration::from_millis(100));
    let mut frame = 0_u8;
    println!("Publishing foxglove.RawImage on {TOPIC}; press Ctrl-C to stop");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = ticks.tick() => {
                const WIDTH: usize = 320;
                const HEIGHT: usize = 240;
                let mut pixels = Vec::with_capacity(WIDTH * HEIGHT * 3);
                for y in 0..HEIGHT {
                    for x in 0..WIDTH {
                        pixels.extend_from_slice(&[
                            (x as u8).wrapping_add(frame),
                            (y as u8).wrapping_add(frame),
                            (x as u8).wrapping_add(y as u8).wrapping_sub(frame),
                        ]);
                    }
                }
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
                let message = json!({
                    "timestamp": { "sec": now.as_secs(), "nsec": now.subsec_nanos() },
                    "frame_id": "camera",
                    "width": WIDTH,
                    "height": HEIGHT,
                    "encoding": "rgb8",
                    "step": WIDTH * 3,
                    "data": STANDARD.encode(pixels),
                });
                session.put(TOPIC, serde_json::to_vec(&message)?)
                    .encoding(zenoh::bytes::Encoding::APPLICATION_JSON.with_schema("foxglove.RawImage"))
                    .await?;
                frame = frame.wrapping_add(3);
            }
        }
    }
    session.close().await?;
    Ok(())
}
