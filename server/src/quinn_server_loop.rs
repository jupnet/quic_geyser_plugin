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
    config::{DictionaryCompressionConfig, QuicParameters},
    defaults::MAX_DATAGRAM_SIZE,
    dictionary::{CompressionDictionary, DictionaryType},
    filters::Filter,
    message::Message,
    stream_manager::StreamBuffer,
    types::{
        account::Account, block_meta::SlotMeta, slot_identifier::SlotIdentifier,
        transaction::CompressedTransaction,
    },
};
use quinn::{Endpoint, EndpointConfig, RecvStream, SendStream, TokioRuntime};
use tokio::{io::AsyncWriteExt, sync::RwLock};

use crate::configure_server::configure_server;
use crate::dictionary_manager::{DictionaryManager, SharedDictionaryManager};

const MAX_NB_OF_MESSAGES: usize = 1_000_000;

pub async fn server_loop(
    keypair: Keypair,
    quic_parameters: QuicParameters,
    socket: SocketAddr,
    data_channel_rx: tokio::sync::broadcast::Receiver<ChannelMessage>,
    compression_type: CompressionType,
    dictionary_config: DictionaryCompressionConfig,
) -> anyhow::Result<()> {
    let nb_of_streams = quic_parameters.max_number_of_streams_per_client as usize;
    let (config, _) = configure_server(quic_parameters, &keypair)?;

    let endpoint = Endpoint::new(
        EndpointConfig::default(),
        Some(config.clone()),
        UdpSocket::bind(socket)?,
        Arc::new(TokioRuntime),
    )?;

    // Create dictionary manager and start training loop
    let dict_manager: SharedDictionaryManager = Arc::new(DictionaryManager::new(dictionary_config));
    if dict_manager.is_enabled() {
        log::info!("Dictionary compression enabled, starting training loop");
        dict_manager.clone().start_training_loop();
    }

    tokio::spawn(async move {
        let data_channel_rx = data_channel_rx;
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
                        dict_manager.clone(),
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
    log_dict: Option<&Arc<CompressionDictionary>>,
    msg_dict: Option<&Arc<CompressionDictionary>>,
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
        ChannelMessage::Transaction(transaction) => {
            // Try to compress transaction if dictionaries are available
            if log_dict.is_some() || msg_dict.is_some() {
                match CompressedTransaction::from_transaction(
                    &transaction,
                    log_dict.map(|d| (d.data.as_slice(), d.id)),
                    msg_dict.map(|d| (d.data.as_slice(), d.id)),
                ) {
                    Ok(compressed) => Message::CompressedTransactionMsg(Box::new(compressed)),
                    Err(e) => {
                        log::debug!("Dictionary compression failed: {:?}, using uncompressed", e);
                        Message::TransactionMsg(transaction)
                    }
                }
            } else {
                Message::TransactionMsg(transaction)
            }
        }
        ChannelMessage::Block(block) => Message::BlockMsg(block),
    }
}

async fn handle_connection(
    connection: quinn::Connection,
    data_channel_tx: tokio::sync::broadcast::Receiver<ChannelMessage>,
    nb_of_streams: usize,
    compression_type: CompressionType,
    dict_manager: SharedDictionaryManager,
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
            dict_manager.clone(),
        ));
    }

    // Send current dictionaries to new client
    if dict_manager.is_enabled() {
        if let Some(dict) = dict_manager.get_current(DictionaryType::LogMessages).await {
            send_dictionary_to_senders(&senders, &dict);
        }
        if let Some(dict) = dict_manager
            .get_current(DictionaryType::TransactionMessage)
            .await
        {
            send_dictionary_to_senders(&senders, &dict);
        }
    }

    // Subscribe to dictionary updates
    let dict_rx = dict_manager.subscribe();

    // start senders
    let dispatcher_task = start_dispatcher(
        senders,
        filters,
        data_channel_tx,
        filters_updated,
        message_enqueued,
        dict_manager.clone(),
        dict_rx,
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

/// Send a dictionary update to all senders
fn send_dictionary_to_senders(
    senders: &VecDeque<tokio::sync::mpsc::UnboundedSender<ChannelMessageOrDict>>,
    dict: &Arc<CompressionDictionary>,
) {
    // Send to first sender only (it will reach the client)
    if let Some(sender) = senders.front() {
        let _ = sender.send(ChannelMessageOrDict::Dictionary(dict.clone()));
    }
}

/// Internal enum for sending both channel messages and dictionary updates
enum ChannelMessageOrDict {
    Message(ChannelMessage),
    Dictionary(Arc<CompressionDictionary>),
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
    senders: VecDeque<tokio::sync::mpsc::UnboundedSender<ChannelMessageOrDict>>,
    filters: Arc<RwLock<Vec<Filter>>>,
    mut data_channel_tx: tokio::sync::broadcast::Receiver<ChannelMessage>,
    filters_updated: Arc<AtomicBool>,
    message_enqueued: Arc<AtomicU64>,
    dict_manager: SharedDictionaryManager,
    mut dict_rx: tokio::sync::broadcast::Receiver<Arc<CompressionDictionary>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut current_filters = { filters.read().await.clone() };

        let mut senders = senders;
        loop {
            tokio::select! {
                message = data_channel_tx.recv() => {
                    if filters_updated.load(Ordering::Relaxed) {
                        current_filters = filters.read().await.clone();
                        filters_updated.store(false, Ordering::Relaxed);
                    }

                    let Ok(message) = message else {
                        log::debug!("data channel closed");
                        break;
                    };

                    // Record transaction for dictionary training
                    if let ChannelMessage::Transaction(ref tx) = message {
                        dict_manager.record_transaction(tx).await;
                    }

                    // if the message is allowed by the filters, transform and send it
                    let matching_filter = current_filters.iter().find(|f| f.allows(&message));
                    if let Some(filter) = matching_filter {
                        let output = filter.transform(message);
                        if let Some(sender) = senders.pop_front() {
                            let _ = sender.send(ChannelMessageOrDict::Message(output));
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
                dict_update = dict_rx.recv() => {
                    // Broadcast dictionary update to client
                    if let Ok(dict) = dict_update {
                        send_dictionary_to_senders(&senders, &dict);
                    }
                }
            }
        }
    })
}

fn create_sender(
    mut stream: SendStream,
    compression_type: CompressionType,
    do_exit: Arc<AtomicBool>,
    message_enqueued: Arc<AtomicU64>,
    dict_manager: SharedDictionaryManager,
) -> tokio::sync::mpsc::UnboundedSender<ChannelMessageOrDict> {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let item: Option<ChannelMessageOrDict> =
                tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                    .await
                    .unwrap_or(None);
            if let Some(item) = item {
                let is_message = matches!(item, ChannelMessageOrDict::Message(_));
                let message = match item {
                    ChannelMessageOrDict::Message(channel_message) => {
                        // Get current dictionaries for compression
                        let (log_dict, msg_dict) =
                            dict_manager.get_current_dictionaries().await;
                        channel_message_to_message(
                            channel_message,
                            compression_type,
                            log_dict.as_ref(),
                            msg_dict.as_ref(),
                        )
                    }
                    ChannelMessageOrDict::Dictionary(dict) => {
                        Message::DictionaryUpdate((*dict).clone())
                    }
                };

                if let Err(e) = stream.write_all(&message.to_binary_stream()).await {
                    log::debug!("Error while sending message : {e:?}");
                    break;
                }
                if let Err(e) = stream.flush().await {
                    log::debug!("Error while flushing message : {e:?}");
                    break;
                }

                // Only decrement for actual messages, not dictionary updates
                if is_message {
                    message_enqueued.fetch_sub(1, Ordering::Relaxed);
                }
            }

            if do_exit.load(Ordering::Relaxed) {
                break;
            }
        }
        do_exit.store(true, Ordering::Relaxed);
    });
    sender
}
