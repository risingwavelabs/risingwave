use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, StreamExt as _};
use once_cell::sync::OnceCell;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::OwnedMessage;
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use tokio::sync::{RwLock, mpsc};

/// Global key == `connection_id` (already unique in metadata)
pub type ReaderKey = String;

pub struct KafkaMuxReader {
    consumer: StreamConsumer,
    /// (topic, partition) -> sender  (each topic unique within this connection)
    senders: RwLock<HashMap<(String, i32), mpsc::Sender<OwnedMessage>>>,
}

static GLOBAL: OnceCell<RwLock<HashMap<ReaderKey, Arc<KafkaMuxReader>>>> = OnceCell::new();

impl KafkaMuxReader {
    fn registry() -> &'static RwLock<HashMap<ReaderKey, Arc<KafkaMuxReader>>> {
        GLOBAL.get_or_init(|| RwLock::new(HashMap::new()))
    }

    /// Create or reuse the reader for a given connection.
    /// `connection_id` : globally unique identifier assigned by metadata.
    /// `brokers`       : comma‑separated list for this connection.
    pub async fn get_or_create(connection_id: ReaderKey, brokers: String) -> Arc<Self> {
        // fast path – already exists
        if let Some(r) = Self::registry().read().await.get(&connection_id).cloned() {
            return r;
        }

        // build single group.id derived from connection id
        let group_id = format!("conn-{connection_id}");
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .create()
            .await
            .expect("failed to create kafka consumer");

        let reader = Arc::new(Self {
            consumer,
            senders: RwLock::new(HashMap::new()),
        });
        Self::registry()
            .write()
            .await
            .insert(connection_id, Arc::clone(&reader));
        tokio::spawn(Self::poll_loop(Arc::clone(&reader)));
        reader
    }

    async fn poll_loop(this: Arc<Self>) {
        let mut stream = this.consumer.stream();
        while let Some(res) = stream.next().await {
            match res {
                Ok(m) => {
                    let owned = m.detach();
                    let key = (owned.topic().to_owned(), owned.partition());
                    if let Some(tx) = this.senders.read().await.get(&key) {
                        let _ = tx.send(owned).await;
                    }
                }
                Err(e) => eprintln!("Kafka error: {e}"),
            }
        }
    }

    pub async fn register_splits(
        &self,
        splits: &[(String, i32)],
    ) -> anyhow::Result<mpsc::Receiver<OwnedMessage>> {
        if splits.is_empty() {
            anyhow::bail!("splits list is empty");
        }
        {
            let map = self.senders.read().await;
            for (t, p) in splits {
                if map.contains_key(&(t.clone(), *p)) {
                    anyhow::bail!("split ({t},{p}) already registered");
                }
            }
        }
        // sender / receiver
        let (tx, rx) = mpsc::channel(1024);
        {
            let mut map = self.senders.write().await;
            for (t, p) in splits {
                map.insert((t.clone(), *p), tx.clone());
            }
        }

        let mut tpl = TopicPartitionList::new();
        for (t, p) in splits {
            tpl.add_partition(t, *p);
        }
        self.consumer
            .incremental_assign(&tpl)
            .map_err(|e| anyhow::anyhow!("assign failed: {e}"))?;
        Ok(rx)
    }

    pub async fn unregister_splits(&self, splits: &[(String, i32)]) -> anyhow::Result<()> {
        if splits.is_empty() {
            return Ok(());
        }
        {
            let map = self.senders.read().await;
            for (t, p) in splits {
                if !map.contains_key(&(t.clone(), *p)) {
                    anyhow::bail!("split ({t},{p}) not registered");
                }
            }
        }
        {
            let mut map = self.senders.write().await;
            for (t, p) in splits {
                map.remove(&(t.clone(), *p));
            }
        }

        let mut tpl = TopicPartitionList::new();
        for (t, p) in splits {
            tpl.add_partition(t, *p);
        }
        self.consumer
            .incremental_unassign(&tpl)
            .map_err(|e| anyhow::anyhow!("unassign failed: {e}"))?;
        Ok(())
    }

    /// Allow `fetch_watermarks` for backfill / seek‑to‑latest use‑cases
    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: i32,
        timeout: Duration,
    ) -> rdkafka::error::KafkaResult<(i64, i64)> {
        self.consumer
            .fetch_watermarks(topic, partition, timeout)
            .await
    }
}
