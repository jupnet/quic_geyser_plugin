use jupnet_sdk::signature::Keypair;
use quic_geyser_common::{
    channel_message::ChannelMessage, config::ConfigQuicPlugin, plugin_error::QuicGeyserError,
};
use std::fmt::Debug;

use super::quinn_server_loop::server_loop;
pub struct QuicServer {
    pub data_channel_sender: tokio::sync::broadcast::Sender<ChannelMessage>,
    pub quic_plugin_config: ConfigQuicPlugin,
    _server_loop_jh: tokio::task::JoinHandle<()>,
}

impl Debug for QuicServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicServer").finish()
    }
}

impl QuicServer {
    pub fn new(
        config: ConfigQuicPlugin,
        keypair: Keypair,
        runtime: &tokio::runtime::Runtime,
    ) -> anyhow::Result<Self> {
        let socket = config.address;
        let compression_type = config.compression_parameters.compression_type;
        let quic_parameters = config.quic_parameters.clone();

        // channel for 32k messages
        let (data_channel_sender, data_channel_tx) = tokio::sync::broadcast::channel(32 * 1024);

        let _server_loop_jh = runtime.spawn(async move {
            if let Err(e) = server_loop(
                keypair,
                quic_parameters,
                socket,
                data_channel_tx,
                compression_type,
            )
            .await
            {
                panic!("Server loop closed by error : {e}");
            }
        });

        Ok(QuicServer {
            data_channel_sender,
            quic_plugin_config: config,
            _server_loop_jh,
        })
    }

    pub fn send_message(&self, message: ChannelMessage) -> Result<(), QuicGeyserError> {
        self.data_channel_sender.send(message).map_err(|e| {
            log::error!("Error sending message: {}", e);
            QuicGeyserError::MessageChannelClosed
        })?;
        Ok(())
    }
}
