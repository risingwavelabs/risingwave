// kafka_mux_split_reader.rs
// Unified KafkaMux model with automatic global lazy initialization (OnceLock + get_or_init)

use std::collections::{HashMap, HashSet, hash_map};
use std::sync::{Arc, OnceLock};

use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use hash_map::Entry;
use itertools::Itertools;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::BorrowedMessage;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use tokio::sync::{RwLock, mpsc};

use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::kafka::{
    KAFKA_ISOLATION_LEVEL, KafkaContextCommon, KafkaProperties, KafkaSplit, RwConsumerContext,
};
use crate::source::{
    BoxSourceChunkStream, Column, SourceContextRef, SplitId, SplitImpl, SplitMetaData, SplitReader,
    into_chunk_stream,
};

static GLOBAL_MUX_CTRL: OnceLock<mpsc::Sender<MuxControl>> = OnceLock::new();

/// Ensure the global `KafkaMux` is initialized, spawning it if needed, and return the control sender.
fn ensure_global_mux() -> mpsc::Sender<MuxControl> {
    GLOBAL_MUX_CTRL
        .get_or_init(|| {
            let (ctrl_tx, ctrl_rx) = mpsc::channel(1024);
            // spawn the manager
            tokio::spawn(async move {
                let mut mux = KafkaMuxReader::new(ctrl_rx).await;
                mux.run().await;
            });
            ctrl_tx
        })
        .clone()
}

// -----------------------------------------------------------------------------
// Control messages for KafkaMux
// -----------------------------------------------------------------------------

#[derive(Debug)]
pub enum MuxControl {
    AddSplits(AddSplitsMessage),
    SeekToLast(SeekToLastMessage),
}

#[derive(Debug)]
pub struct AddSplitsMessage {
    source: u64,

    properties: KafkaProperties,
    splits: Vec<KafkaSplit>,

    sender: mpsc::Sender<SourceMessage>,
}

#[derive(Debug)]
pub struct SeekToLastMessage {
    source: u64,

    properties: KafkaProperties,
    splits: Vec<KafkaSplit>,

    sender: mpsc::Sender<SourceMessage>,
}

pub type SourceId = u64;

pub type ConsumerRef = Arc<StreamConsumer<RwConsumerContext>>;

pub struct ConsumerInfo {
    consumer: ConsumerRef,

    assigned_splits: HashSet<SplitId>,
    // unique cluster
}

// -----------------------------------------------------------------------------
// KafkaMux: multiplex few consumers, demux messages to per-source channels
// -----------------------------------------------------------------------------

pub type InternalClusterId = u64;
pub struct KafkaMuxReader<'a> {
    /// dynamic pool of consumers, initially empty
    consumers: HashMap<InternalClusterId, ConsumerInfo>,

    cluster_mapping: HashMap<SourceId, InternalClusterId>,

    /// routes: (topic, partition) -> channel sender
    routes: Arc<RwLock<HashMap<(SourceId, String, i32), mpsc::Sender<SourceMessage>>>>,
    /// control commands
    control_rx: mpsc::Receiver<MuxControl>,
    /// channel for demultiplexed messages
    message_rx: mpsc::Receiver<(SourceId, BorrowedMessage<'a>)>,
    message_tx: mpsc::Sender<(SourceId, BorrowedMessage<'a>)>,
}

impl<'a> KafkaMuxReader<'a> {
    /// Create new manager: no initial consumers, but setup message channel
    pub async fn new(control_rx: mpsc::Receiver<MuxControl>) -> Self {
        let (message_tx, message_rx) = mpsc::channel(1024);
        KafkaMuxReader {
            consumers: Default::default(),
            cluster_mapping: Default::default(),
            routes: Arc::new(Default::default()),
            control_rx,
            message_rx,
            message_tx,
        }
    }

    /// Single select! loop to handle incoming Kafka messages & control commands
    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                // demultiplexed message arrives
                Some((source_id, msg)) = self.message_rx.recv() => {
                    let key = (source_id, msg.topic().to_owned(), msg.partition());
                    let guard = self.routes.read().await;
                    if let Some(tx) = guard.get(&key) {
                        let msg = SourceMessage::from_kafka_message(&msg,false);
                        let _ = tx.send(msg).await;
                    }
                }
                // control command arrives
                Some(ctrl) = self.control_rx.recv() => match ctrl {
                    MuxControl::AddSplits(msg) => {
                        self.handle_add(msg).await.unwrap();
                    }
                },
                else => break,
            }
        }
    }

    /// Handle new splits: create or reuse consumer, assign+pause, spawn its poll
    async fn handle_add(&mut self, msg: AddSplitsMessage) -> Result<()> {
        let AddSplitsMessage {
            source,
            properties,
            splits,
            sender,
        } = msg;

        if let Entry::Vacant(e) = self.consumers.entry(source) {
            let consumer = Self::create_source(properties).await?;

            e.insert(ConsumerInfo {
                consumer,
                assigned_splits: splits.iter().map(|split| split.id()).collect(),
            });

            return Ok(());
        }

        let consumer = self.consumers.get(&source).expect("123");

        for split in &splits {
            assert!(!consumer.assigned_splits.contains(&split.id()))
        }

        let new_splits = splits
            .into_iter()
            .filter(|split| !consumer.assigned_splits.contains(&split.id()))
            .collect_vec();

        let mut tpl = TopicPartitionList::new();

        for split in &new_splits {
            if let Some(offset) = split.start_offset {
                tpl.add_partition_offset(
                    split.topic.as_str(),
                    split.partition,
                    Offset::Offset(offset + 1),
                )?;
            } else {
                tpl.add_partition(split.topic.as_str(), split.partition);
            }
        }

        consumer.consumer.incremental_assign(&tpl)?;

        let mut guard = self.routes.write().await;
        for split in new_splits {
            guard.insert((source, split.topic, split.partition), sender.clone());
        }

        Ok(())
    }

    async fn create_source(properties: KafkaProperties) -> Result<ConsumerRef> {
        let mut config = ClientConfig::new();

        let bootstrap_servers = &properties.connection.brokers;
        let broker_rewrite_map = properties.privatelink_common.broker_rewrite_map.clone();

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("auto.offset.reset", "smallest");
        config.set("isolation.level", KAFKA_ISOLATION_LEVEL);
        config.set("bootstrap.servers", bootstrap_servers);

        properties.connection.set_security_properties(&mut config);
        properties.set_client(&mut config);

        //        config.set("group.id", properties.group_id(source_ctx.fragment_id));

        let ctx_common = KafkaContextCommon::new(
            broker_rewrite_map,
            Some(format!(
                "fragment-{}-source-{}-actor-{}",
                // source_ctx.fragment_id, source_ctx.source_id, source_ctx.actor_id
                1,
                2,
                3
            )),
            // thread consumer will keep polling in the background, we don't need to call `poll`
            // explicitly
            // Some(source_ctx.metrics.rdkafka_native_metric.clone()),
            None,
            properties.aws_auth_props,
            properties.connection.is_aws_msk_iam(),
        )
        .await?;

        let client_ctx = RwConsumerContext::new(ctx_common);
        let consumer: StreamConsumer<RwConsumerContext> = config
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(client_ctx)
            .await
            .context("failed to create kafka consumer")?;

        Ok(Arc::new(consumer))
    }
}

// -----------------------------------------------------------------------------
// SplitReader using automatic global KafkaMux
// -----------------------------------------------------------------------------
pub struct KafkaMuxSplitReader {
    data_rx: mpsc::Receiver<SourceMessage>,
    parser: ParserConfig,
    ctx: SourceContextRef,
    // backfill_info: HashMap<SplitId, BackfillInfo>,
    splits: Vec<KafkaSplit>,
}

#[async_trait]
impl SplitReader for KafkaMuxSplitReader {
    type Properties = KafkaProperties;
    type Split = KafkaSplit;

    async fn new(
        properties: KafkaProperties,
        splits: Vec<KafkaSplit>,
        parser_cfg: ParserConfig,
        source_ctx: SourceContextRef,
        _cols: Option<Vec<Column>>,
    ) -> Result<Self> {
        // register with global mux
        let ctrl = ensure_global_mux();
        let (tx, data_rx) = mpsc::channel(1024);
        let msg = AddSplitsMessage {
            source: source_ctx.source_id.table_id as u64,
            properties: properties.clone(),
            splits: splits.clone(),
            sender: tx,
        };

        ctrl.send(MuxControl::AddSplits(msg)).await.unwrap();
        Ok(KafkaMuxSplitReader {
            data_rx,
            parser: parser_cfg,
            ctx: source_ctx,
            // backfill_info,
            splits,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        let parser_config = self.parser.clone();
        let source_context = self.ctx.clone();

        into_chunk_stream(self.into_data_stream(), parser_config, source_context)
    }

    async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {}
}

impl KafkaMuxSplitReader {
    #[try_stream(ok = Vec<SourceMessage>, error = crate::error::ConnectorError)]
    async fn into_data_stream(self) {
        let mut rx = self.data_rx;
        while let Some(msg) = rx.recv().await {
            yield vec![msg];
        }
    }
}
