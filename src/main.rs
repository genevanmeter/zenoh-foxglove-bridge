use zenoh_plugin_foxglove::{Bridge, BridgeConfig, Result};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let mut config = BridgeConfig::default();
    let mut zenoh_config = zenoh::Config::default();
    let mut descriptor_file = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            println!(
                "zenoh-foxglove-bridge [--config bridge.json] [--zenoh-config zenoh.json5] [--descriptor-file descriptor.bin]\n\nDefault: subscribe ** and serve ws://127.0.0.1:8765\nBridge settings: bind, port, key_expr, message_types, descriptor_file,\nquery_timeout_ms, retry_interval_ms, queue_capacity, max_topics, max_message_bytes."
            );
            return Ok(());
        }
        match arg.as_str() {
            "--config" => {
                config = serde_json::from_slice(&std::fs::read(
                    args.next().ok_or("--config requires a path")?,
                )?)?
            }
            "--zenoh-config" => {
                zenoh_config =
                    zenoh::Config::from_file(args.next().ok_or("--zenoh-config requires a path")?)?
            }
            "--descriptor-file" => {
                descriptor_file = Some(args.next().ok_or("--descriptor-file requires a path")?)
            }
            _ => return Err(format!("unknown argument {arg}; use --help").into()),
        }
    }
    if descriptor_file.is_some() {
        config.descriptor_file = descriptor_file;
    }
    let session = zenoh::open(zenoh_config).await?;
    let bridge = Bridge::start(session, config).await?;
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result?, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    bridge.shutdown().await;
    Ok(())
}
