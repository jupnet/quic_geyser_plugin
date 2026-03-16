use clap::Parser;
use quic_geyser_common::defaults::DEFAULT_MAX_STREAMS;

#[derive(Parser, Debug, Clone)]
#[clap(name = "quic_plugin_tester")]
pub struct Args {
    #[clap(short, long)]
    pub url: String,

    #[clap(short, long)]
    pub rpc_url: Option<String>,

    #[clap(short, long, default_value_t = false)]
    pub blocks_instead_of_accounts: bool,

    #[clap(short = 's', long, default_value_t = DEFAULT_MAX_STREAMS)]
    pub number_of_streams: u64,

    /// Subscribe to TransactionNotifyByProgram for this program id (base58).
    /// Sends only signature, status, CU consumed and optionally the message.
    #[clap(long)]
    pub notify_program: Option<String>,

    /// When using --notify-program, also include the transaction message.
    #[clap(long, default_value_t = false)]
    pub notify_include_message: bool,

    /// Subscribe to TransactionAllProgram for this program id (base58).
    /// Sends the full transaction filtered by program.
    #[clap(long)]
    pub transaction_program: Option<String>,
}
