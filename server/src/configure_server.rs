use std::sync::Arc;

use jupnet_sdk::signature::Keypair;
use jupnet_streamer::{quic::SkipClientVerification, tls_certificates::new_dummy_x509_certificate};
use pem::Pem;
use quic_geyser_common::{
    config::QuicParameters,
    defaults::{ALPN_GEYSER_PROTOCOL_ID, MAX_DATAGRAM_SIZE},
};
use quinn::{crypto::rustls::QuicServerConfig, IdleTimeout, VarInt};
use quinn_proto::ServerConfig;
use rustls::KeyLogFile;

pub(crate) fn configure_server(
    quic_parameters: QuicParameters,
    identity_keypair: &Keypair,
) -> anyhow::Result<(ServerConfig, String)> {
    let (cert, priv_key) = new_dummy_x509_certificate(identity_keypair);
    let cert_chain_pem_parts = vec![Pem {
        tag: "CERTIFICATE".to_string(),
        contents: cert.as_ref().to_vec(),
    }];
    let cert_chain_pem = pem::encode_many(&cert_chain_pem_parts);

    let mut server_tls_config = rustls::ServerConfig::builder()
        .with_client_cert_verifier(SkipClientVerification::new())
        .with_single_cert(vec![cert], priv_key)?;
    server_tls_config.alpn_protocols = vec![ALPN_GEYSER_PROTOCOL_ID.to_vec()];
    server_tls_config.key_log = Arc::new(KeyLogFile::new());
    let quic_server_config = QuicServerConfig::try_from(server_tls_config)?;

    let mut server_config = ServerConfig::with_crypto(Arc::new(quic_server_config));
    let config = Arc::get_mut(&mut server_config.transport).unwrap();

    // QUIC_MAX_CONCURRENT_STREAMS doubled, which was found to improve reliability
    let timeout: VarInt = ((quic_parameters.connection_timeout * 1000) as u32).into();
    let idle_timeout = IdleTimeout::try_from(timeout).unwrap();
    config.max_idle_timeout(Some(idle_timeout));
    config.max_concurrent_uni_streams((1 as u32).into());
    config.stream_receive_window((quic_parameters.recieve_window_size as u32).into());
    config.crypto_buffer_size(64 * 1024); // 64 Kbs
    config.send_window(32 * 1024 * 1024); // 32 MBs
    config.enable_segmentation_offload(quic_parameters.enable_gso);
    // for getting 64 transasction streams in parallel
    config.receive_window((quic_parameters.recieve_window_size as u32).into());
    config.initial_mtu(MAX_DATAGRAM_SIZE as u16);

    // disable bidi & datagrams
    const MAX_CONCURRENT_BIDI_STREAMS: u32 = 0;
    config.max_concurrent_bidi_streams(MAX_CONCURRENT_BIDI_STREAMS.into());
    config.datagram_receive_buffer_size(None);

    Ok((server_config, cert_chain_pem))
}
