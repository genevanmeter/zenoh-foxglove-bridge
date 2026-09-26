use crate::{Bridge, BridgeConfig};
use zenoh::{
    internal::plugins::{RunningPlugin, RunningPluginTrait, ZenohPlugin},
    internal::runtime::DynamicRuntime,
};
use zenoh_plugin_trait::{Plugin, PluginControl};

pub struct FoxglovePlugin;
zenoh_plugin_trait::declare_plugin!(FoxglovePlugin);
impl ZenohPlugin for FoxglovePlugin {}

impl Plugin for FoxglovePlugin {
    type StartArgs = DynamicRuntime;
    type Instance = RunningPlugin;
    const DEFAULT_NAME: &'static str = "foxglove";
    const PLUGIN_VERSION: &'static str = env!("CARGO_PKG_VERSION");
    const PLUGIN_LONG_VERSION: &'static str = env!("CARGO_PKG_VERSION");

    fn start(name: &str, runtime: &DynamicRuntime) -> zenoh::Result<RunningPlugin> {
        let mut value = runtime.get_config().get_plugin_config(name)?;
        // These keys belong to zenohd's plugin loader, not BridgeConfig.
        if let Some(object) = value.as_object_mut() {
            object.retain(|key, _| !key.starts_with("__"));
        }
        let config: BridgeConfig = serde_json::from_value(value)?;
        let runtime = runtime.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("foxglove-plugin".into())
            .spawn(move || {
                let result = (|| -> crate::Result<()> {
                    let executor = tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()?;
                    executor.block_on(async {
                        let session = zenoh::session::init(runtime).await?;
                        let bridge = Bridge::start(session, config).await?;
                        let _ = ready_tx.send(Ok(()));
                        let _ = stop_rx.await;
                        bridge.shutdown().await;
                        Ok(())
                    })
                })();
                if let Err(error) = result {
                    let _ = ready_tx.send(Err(error.to_string()));
                }
            })?;
        match ready_rx.recv()? {
            Ok(()) => Ok(Box::new(Instance {
                stop: Some(stop_tx),
                thread: Some(thread),
            })),
            Err(error) => {
                let _ = thread.join();
                Err(error.into())
            }
        }
    }
}

struct Instance {
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl PluginControl for Instance {}
impl RunningPluginTrait for Instance {}
impl Drop for Instance {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
