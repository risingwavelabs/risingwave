// kafka_mux_split_reader.rs
// Unified KafkaMux model without dashmap: multiplex Kafka consumers + demux to per-source readers

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::for_await;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use tokio::sync::{RwLock, mpsc};

use crate::error::ConnectorResult as Result;
use crate::parser::ParserConfig;
use crate::source::base::SourceMessage;
use crate::source::kafka::{KafkaContextCommon, KafkaProperties, KafkaSplit, RwConsumerContext};
use crate::source::{
    BackfillInfo, BoxSourceChunkStream, Column, SourceContextRef, SplitId, SplitImpl, SplitReader,
    into_chunk_stream,
};

// -----------------------------------------------------------------------------
// Control messages for KafkaMux
// -----------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum MuxControl {
    AddSplits {
        epoch: u64,
        source: u64,
        splits: Vec<KafkaSplit>,
    },
    ActivateSplits {
        epoch: u64,
    },
    RemoveSplits {
        epoch: u64,
        splits: Vec<KafkaSplit>,
    },
    StopSource {
        source: u64,
    },
}

// -----------------------------------------------------------------------------
// KafkaMux: multiplex few consumers, demux messages to per-source channels
// -----------------------------------------------------------------------------

pub struct KafkaMux {
    consumers: Vec<Arc<StreamConsumer<RwConsumerContext>>>,
    /// (topic, partition) -> channel sender
    routes: Arc<RwLock<HashMap<(String, i32), mpsc::Sender<Arc<dyn Message>>>>>,
    control_rx: mpsc::Receiver<MuxControl>,
    max_parts_per_consumer: usize,
}

impl KafkaMux {
    pub async fn new(control_rx: mpsc::Receiver<MuxControl>, max_parts: usize) -> Self {
        let initial = Arc::new(Self::create_consumer()?);
        KafkaMux {
            consumers: vec![initial],
            routes: Arc::new(RwLock::new(HashMap::new())),
            control_rx,
            max_parts_per_consumer: max_parts,
        }
    }

    pub async fn run(mut self) {
        // Spawn polling tasks for each consumer
        for consumer in &self.consumers {
            let c = consumer.clone();
            let routes = Arc::clone(&self.routes);
            tokio::spawn(async move {
                while let Ok(msg) = c.recv().await {
                    let key = (msg.topic().to_string(), msg.partition());
                    let guard = routes.read().await;
                    if let Some(tx) = guard.get(&key) {
                        let _ = tx.send(msg.detach()).await;
                    }
                }
            });
        }

        // Control-plane loop
        while let Some(ctrl) = self.control_rx.recv().await {
            match ctrl {
                MuxControl::AddSplits {
                    epoch: _,
                    source: _,
                    splits,
                } => {
                    // choose or create consumer
                    let mut chosen = None;
                    for c in &self.consumers {
                        let assigned = c.assignment().map(|l| l.count()).unwrap_or(0) as usize;
                        if assigned + splits.len() <= self.max_parts_per_consumer {
                            chosen = Some(c.clone());
                            break;
                        }
                    }
                    let consumer = chosen.unwrap_or_else(|| {
                        let nc = Arc::new(Self::create_consumer().unwrap());
                        self.consumers.push(nc.clone());
                        nc
                    });
                    let tpl = Self::to_topic_partition_list(&splits);
                    consumer.incremental_assign(&tpl).unwrap();
                    consumer.pause(&tpl).unwrap();

                    // register routes
                    let mut guard = self.routes.write().await;
                    for split in splits {
                        let (t, p) = (split.topic.clone(), split.partition);
                        let (tx, _) = mpsc::channel(1024);
                        guard.insert((t, p), tx.clone());
                    }
                }
                MuxControl::ActivateSplits { epoch: _ } => {
                    let guard = self.routes.read().await;
                    let keys: Vec<_> = guard.keys().cloned().collect();
                    drop(guard);
                    let splits: Vec<KafkaSplit> = keys
                        .into_iter()
                        .map(|(t, p)| KafkaSplit {
                            topic: t,
                            partition: p,
                            start_offset: None,
                            stop_offset: None,
                        })
                        .collect();
                    let tpl = Self::to_topic_partition_list(&splits);
                    if let Some(c) = self.consumers.first() {
                        c.resume(&tpl).unwrap();
                    }
                }
                MuxControl::RemoveSplits { epoch: _, splits } => {
                    let tpl = Self::to_topic_partition_list(&splits);
                    if let Some(c) = self.consumers.first() {
                        c.incremental_unassign(&tpl).unwrap();
                    }
                    let mut guard = self.routes.write().await;
                    for split in splits {
                        guard.remove(&(split.topic, split.partition));
                    }
                }
                MuxControl::StopSource { source: _ } => {
                    // handle graceful source stop if needed
                }
            }
        }
    }

    fn create_consumer() -> Result<StreamConsumer<RwConsumerContext>> {
        let mut cfg = ClientConfig::new();
        // ... set common Kafka configs
        cfg.create_with_context(RwConsumerContext::new(KafkaContextCommon::default())?)
            .context("failed to create consumer")
    }

    fn to_topic_partition_list(splits: &[KafkaSplit]) -> TopicPartitionList {
        let mut tpl = TopicPartitionList::new();
        for s in splits {
            let offset = s
                .start_offset
                .map_or(Offset::Beginning, |o| Offset::Offset(o + 1));
            tpl.add_partition_offset(&s.topic, s.partition, offset)
                .unwrap();
        }
        tpl
    }
}

// -----------------------------------------------------------------------------
// SplitReader using KafkaMux
// -----------------------------------------------------------------------------

pub struct KafkaMuxSplitReader {
    splits: Vec<KafkaSplit>,
    offsets: HashMap<SplitId, (Option<i64>, Option<i64>)>,
    backfill: HashMap<SplitId, BackfillInfo>,
    data_rx: mpsc::Receiver<Arc<dyn Message>>,
    parser: ParserConfig,
    ctx: SourceContextRef,
}

#[async_trait]
impl SplitReader for KafkaMuxSplitReader {
    type Properties = KafkaProperties;
    type Split = KafkaSplit;

    async fn new(
        props: KafkaProperties,
        splits: Vec<KafkaSplit>,
        parser_cfg: ParserConfig,
        ctx: SourceContextRef,
        _cols: Option<Vec<Column>>,
    ) -> Result<Self> {
        // compute offsets/backfill as before
        let mut offsets = HashMap::new();
        let mut backfill = HashMap::new();
        for split in &splits {
            offsets.insert(split.id(), (split.start_offset, split.stop_offset));
            // fetch watermarks...
        }
        // register with global mux
        let ctrl_tx = ctx.global_mux_control.clone();
        let (tx, data_rx) = mpsc::channel(1024);
        ctrl_tx
            .send(MuxControl::AddSplits {
                epoch: ctx.checkpoint_epoch,
                source: ctx.source_id,
                splits: splits.clone(),
            })
            .await
            .unwrap();
        // ctx should store (topic,partition)->tx mapping for KafkaMux
        Ok(KafkaMuxSplitReader {
            splits,
            offsets,
            backfill,
            data_rx,
            parser: parser_cfg,
            ctx,
        })
    }

    fn into_stream(self) -> BoxSourceChunkStream {
        into_chunk_stream(self.into_data_stream(), self.parser, self.ctx)
    }

    #[for_await]
    async fn into_data_stream(self) {
        while let Some(msg) = self.data_rx.recv().await {
            let sm = SourceMessage::from_kafka_message(&*msg, false);
            yield vec![sm];
        }
    }

    fn backfill_info(&self) -> HashMap<SplitId, BackfillInfo> {
        self.backfill.clone()
    }

    async fn seek_to_latest(&mut self) -> Result<Vec<SplitImpl>> {
        // optionally send RemoveSplits then AddSplits with new offsets
        Ok(Vec::new())
    }
}
