//! Publishes a procedural RGB image using Foxglove's Protobuf RawImage schema.
use foxglove::{
    Encode,
    messages::{RawImage, Timestamp},
};
use prost::Message;
use std::time::Duration;
use zenoh_plugin_foxglove::Result;

const TOPIC: &str = "demo/foxglove/raw_image_proto";
const MESSAGE_TYPE: &str = "foxglove.RawImage";

#[tokio::main]
async fn main() -> Result<()> {
    let config = match std::env::args().nth(1) {
        Some(path) => zenoh::Config::from_file(path)?,
        None => zenoh::Config::default(),
    };
    let session = zenoh::open(config).await?;
    let descriptor = RawImage::get_schema()
        .expect("foxglove.RawImage has a Protobuf schema")
        .data
        .into_owned();
    let queryable = session
        .declare_queryable(format!("{TOPIC}/protobuf_descriptor"))
        .await?;
    let query_task = tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            if let Err(error) = query
                .reply(query.key_expr(), descriptor.clone())
                .encoding(zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema(MESSAGE_TYPE))
                .await
            {
                eprintln!("descriptor reply failed: {error}");
            }
        }
    });

    let mut ticks = tokio::time::interval(Duration::from_millis(100));
    let mut frame = 0_u8;
    println!("Publishing {MESSAGE_TYPE} on {TOPIC}; press Ctrl-C to stop");
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
                let image = RawImage {
                    timestamp: Some(Timestamp::now()),
                    frame_id: "camera".into(),
                    width: WIDTH as u32,
                    height: HEIGHT as u32,
                    encoding: "rgb8".into(),
                    step: (WIDTH * 3) as u32,
                    data: pixels.into(),
                };
                session.put(TOPIC, image.encode_to_vec())
                    .encoding(
                        zenoh::bytes::Encoding::APPLICATION_PROTOBUF.with_schema(MESSAGE_TYPE),
                    )
                    .await?;
                frame = frame.wrapping_add(3);
            }
        }
    }
    query_task.abort();
    session.close().await?;
    Ok(())
}
