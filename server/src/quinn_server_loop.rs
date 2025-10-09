use std::{
    collections::VecDeque,
    net::{SocketAddr, UdpSocket},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use jupnet_sdk::signature::Keypair;
use quic_geyser_common::{
    channel_message::ChannelMessage,
    compression::CompressionType,
    config::QuicParameters,
    defaults::MAX_DATAGRAM_SIZE,
    filters::Filter,
    message::Message,
    stream_manager::StreamBuffer,
    types::{account::Account, block_meta::SlotMeta, slot_identifier::SlotIdentifier},
};
use quinn::{Endpoint, EndpointConfig, RecvStream, SendStream, TokioRuntime};
use tokio::{io::AsyncWriteExt, sync::RwLock};

use crate::configure_server::configure_server;

const MAX_NB_OF_MESSAGES: usize = 1_000_000;

pub fn server_loop(
    keypair: Keypair,
    quic_parameters: QuicParameters,
    socket: SocketAddr,
    data_channel_rx: tokio::sync::broadcast::Receiver<ChannelMessage>,
    compression_type: CompressionType,
) -> anyhow::Result<()> {
    let nb_of_streams = quic_parameters.max_number_of_streams_per_client as usize;
    let (config, _) = configure_server(quic_parameters, &keypair)?;

    let endpoint = Endpoint::new(
        EndpointConfig::default(),
        Some(config.clone()),
        UdpSocket::bind(socket)?,
        Arc::new(TokioRuntime),
    )?;

    tokio::spawn(async move {
        loop {
            let incoming = endpoint.accept().await;
            if let Some(incoming) = incoming {
                let connection = incoming.await;
                if let Ok(connection) = connection {
                    log::info!("received connection from {}", connection.remote_address());
                    tokio::spawn(handle_connection(
                        connection,
                        data_channel_rx.resubscribe(),
                        nb_of_streams,
                        compression_type,
                    ));
                }
            }
        }
    });

    Ok(())
}

fn channel_message_to_message(
    message: ChannelMessage,
    compression_type: CompressionType,
) -> Message {
    match message {
        ChannelMessage::Account(account, slot, _init) => {
            let slot_identifier = SlotIdentifier { slot };
            let geyser_account = Account::new(
                account.pubkey,
                account.account,
                compression_type,
                slot_identifier,
                account.write_version,
            );

            Message::AccountMsg(geyser_account)
        }
        ChannelMessage::Slot(slot, parent, slot_status) => Message::SlotMsg(SlotMeta {
            slot,
            parent,
            slot_status,
        }),
        ChannelMessage::BlockMeta(block_meta) => Message::BlockMetaMsg(block_meta),
        ChannelMessage::Transaction(transaction) => Message::TransactionMsg(transaction),
        ChannelMessage::Block(block) => Message::BlockMsg(block),
    }
}

async fn handle_connection(
    connection: quinn::Connection,
    data_channel_tx: tokio::sync::broadcast::Receiver<ChannelMessage>,
    nb_of_streams: usize,
    compression_type: CompressionType,
) -> anyhow::Result<()> {
    let message_enqueued = Arc::new(AtomicU64::new(0));
    let stream = connection.accept_uni().await?;
    log::info!("accepted stream from {}", connection.remote_address());
    let do_exit = Arc::new(AtomicBool::new(false));
    let filters_updated = Arc::new(AtomicBool::new(false));

    let filters = Arc::new(RwLock::new(Vec::<Filter>::new()));

    // start reading stream for filters
    let filters_task = read_stream_for_filters(
        stream,
        filters.clone(),
        filters_updated.clone(),
        do_exit.clone(),
    );

    let mut senders = VecDeque::with_capacity(nb_of_streams);
    for _ in 0..nb_of_streams {
        let send_stream = connection.open_uni().await?;
        senders.push_back(create_sender(
            send_stream,
            compression_type,
            do_exit.clone(),
            message_enqueued.clone(),
        ));
    }

    // start senders
    let dispatcher_task = start_dispatcher(
        senders,
        filters,
        data_channel_tx,
        filters_updated,
        message_enqueued,
    );

    tokio::select! {
        _ = filters_task => {
            log::info!("filters task finished");
        }
        _ = dispatcher_task => {
            log::info!("dispatcher task finished");
        }
    }
    do_exit.store(true, Ordering::Relaxed);
    Ok(())
}

fn read_stream_for_filters(
    mut stream: RecvStream,
    filters: Arc<RwLock<Vec<Filter>>>,
    filters_updated: Arc<AtomicBool>,
    do_exit: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // buffer for getting filters
        let mut circular_buffer = StreamBuffer::<131072>::new(); // 128kb

        loop {
            if do_exit.load(Ordering::Relaxed) {
                log::debug!("exiting read_stream_for_filters");
                break;
            }

            let chunk = stream.read_chunk(MAX_DATAGRAM_SIZE, true).await;
            let Ok(chunk) = chunk else {
                // connection closed
                log::debug!("connection closed");
                break;
            };

            if let Some(data) = chunk {
                circular_buffer.append_bytes(&data.bytes);
            } else {
                // stream closed
                log::debug!("stream closed");
                break;
            }

            if circular_buffer.len() > 0 {
                let data = circular_buffer.as_buffer();
                let message = Message::from_binary_stream(&data);
                if let Some((message, len)) = message {
                    circular_buffer.consume(len);
                    // we get filters from the client
                    if let Message::Filters(new_filters) = message {
                        let mut lk = filters.write().await;
                        lk.extend(new_filters);
                        filters_updated.store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    })
}

fn start_dispatcher(
    senders: VecDeque<tokio::sync::mpsc::UnboundedSender<ChannelMessage>>,
    filters: Arc<RwLock<Vec<Filter>>>,
    mut data_channel_tx: tokio::sync::broadcast::Receiver<ChannelMessage>,
    filters_updated: Arc<AtomicBool>,
    message_enqueued: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut current_filters = { filters.read().await.clone() };

        let mut senders = senders;
        loop {
            let message = data_channel_tx.recv().await;

            if filters_updated.load(Ordering::Relaxed) {
                current_filters = filters.read().await.clone();
                filters_updated.store(false, Ordering::Relaxed);
            }

            let Ok(message) = message else {
                log::debug!("data channel closed");
                break;
            };

            // if the message is allowed by the filters, send it to the data channel
            if current_filters.iter().any(|f| f.allows(&message)) {
                if let Some(sender) = senders.pop_front() {
                    let _ = sender.send(message);
                    // round robin senders
                    senders.push_back(sender);
                    message_enqueued.fetch_add(1, Ordering::Relaxed);
                } else {
                    log::error!("NB_OF_STREAMS is not enough");
                    break;
                }
            }

            if message_enqueued.load(Ordering::Relaxed) as usize > MAX_NB_OF_MESSAGES {
                log::error!("MAX_NB_OF_MESSAGES reached (connection dropped)");
                break;
            }
        }
    })
}

fn create_sender(
    mut stream: SendStream,
    compression_type: CompressionType,
    do_exit: Arc<AtomicBool>,
    message_enqueued: Arc<AtomicU64>,
) -> tokio::sync::mpsc::UnboundedSender<ChannelMessage> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let message: Option<ChannelMessage> =
                tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                    .await
                    .unwrap_or(None);
            if let Some(channel_message) = message {
                let message = channel_message_to_message(channel_message, compression_type);
                if let Err(e) = stream.write_all(&message.to_binary_stream()).await {
                    log::error!("Error while sending message : {e:?}");
                    break;
                }
                if let Err(e) = stream.flush().await {
                    log::error!("Error while flushing message : {e:?}");
                    break;
                }
                message_enqueued.fetch_sub(1, Ordering::Relaxed);
            }

            if do_exit.load(Ordering::Relaxed) {
                break;
            }
        }
        do_exit.store(true, Ordering::Relaxed);
    });
    sender
}
