use {clap::Parser, quic_geyser_plugin::config::Config};

#[derive(Debug, Parser)]
#[clap(author, version, about)]
struct Args {
    #[clap(short, long, default_value_t = String::from("config.json"))]
    /// Path to config
    config: String,

    #[clap(short, long)]
    print_example: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    if args.print_example {
        use quic_geyser_common::config::{CompressionParameters, ConfigQuicPlugin, QuicParameters};
        use quic_geyser_plugin::config::Config;
        use serde_json::json;
        use std::net::{Ipv4Addr, SocketAddrV4};
        let config = Config {
            libpath: "temp".to_string(),
            quic_plugin: ConfigQuicPlugin {
                address: std::net::SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 10800)),
                quic_parameters: QuicParameters {
                    max_number_of_streams_per_client: 1024,
                    recieve_window_size: 1_000_000,
                    connection_timeout: 600,
                    max_ack_delay: 100,
                    ack_exponent: 100,
                    enable_pacing: true,
                    cc_algorithm: "cubic".to_string(),
                    incremental_priority: true,
                    enable_gso: true,
                    discover_pmtu: true,
                    disconnect_laggy_client: true,
                    max_number_of_connections: 100,
                },
                compression_parameters: CompressionParameters {
                    compression_type: quic_geyser_common::compression::CompressionType::Lz4Fast(8),
                },
                number_of_retries: 100,
                allow_accounts: true,
                allow_accounts_at_startup: false,
                enable_block_builder: false,
                build_blocks_with_accounts: false,
                log_level: "info".to_string(),
            },
        };
        println!("{}", json!(config));
        return Ok(());
    }

    let _config = Config::load_from_file(args.config)?;
    println!("Config is OK!");
    Ok(())
}
