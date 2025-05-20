use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use once_cell::sync::OnceCell;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, TopicPartitionList};
use tokio::sync::{RwLock, mpsc};

/// 用户提供的全局 reader 标识键
pub type ReaderKey = String;

/// Kafka 连接配置
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
}

/// 从 Kafka 转发给各分片的消息结构
pub struct ConsumerRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
}

/// 多路复用读者：同一 compute node 上同一 key 共享连接
pub struct KafkaMuxReader {
    consumer: StreamConsumer,
    split_senders: RwLock<HashMap<(String, i32), mpsc::Sender<ConsumerRecord>>>,
}

/// 全局唯一 map：ReaderKey -> KafkaMuxReader
static GLOBAL_READERS: OnceCell<RwLock<HashMap<ReaderKey, Arc<KafkaMuxReader>>>> = OnceCell::new();

impl KafkaMuxReader {
    fn global_map() -> &'static RwLock<HashMap<ReaderKey, Arc<KafkaMuxReader>>> {
        GLOBAL_READERS.get_or_init(|| RwLock::new(HashMap::new()))
    }

    /// 根据用户提供的 key 获取或创建共享 MuxReader
    pub async fn get_or_create(key: ReaderKey, config: KafkaConfig) -> Arc<KafkaMuxReader> {
        let map = Self::global_map();
        {
            let readers = map.read().await;
            if let Some(existing) = readers.get(&key) {
                return Arc::clone(existing);
            }
        }
        // 创建新的 Kafka Consumer
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .create()
            .await
            .expect("Kafka Consumer 创建失败");
        let reader = Arc::new(KafkaMuxReader {
            consumer,
            split_senders: RwLock::new(HashMap::new()),
        });
        // 写锁插入全局 map
        {
            let mut writers = map.write().await;
            writers.insert(key.clone(), Arc::clone(&reader));
        }
        // 启动后台读取任务
        let reader_clone = Arc::clone(&reader);
        tokio::spawn(async move {
            reader_clone.read_loop().await;
        });
        reader
    }

    /// 移除指定 key 的 reader（如无需再使用）
    pub async fn remove(key: &ReaderKey) {
        let map = Self::global_map();
        let mut writers = map.write().await;
        if let Some(reader) = writers.remove(key) {
            // 可在此执行额外 cleanup，如果需要的话。
            drop(reader);
        }
    }

    /// 注册一个分片 (topic, partition)，使用增量订阅
    pub async fn register_split(
        &self,
        topic: String,
        partition: i32,
    ) -> mpsc::Receiver<ConsumerRecord> {
        let (tx, rx) = mpsc::channel(100);
        let mut senders = self.split_senders.write().await;
        senders.insert((topic.clone(), partition), tx);
        // 增量订阅新分区
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(&topic, partition);
        self.consumer
            .incremental_assign(&tpl)
            .expect("增量订阅失败");
        rx
    }

    /// 注销一个分片，并使用增量取消订阅
    pub async fn unregister_split(&self, topic: &str, partition: i32) {
        {
            let mut senders = self.split_senders.write().await;
            senders.remove(&(topic.to_string(), partition));
        }
        // 增量取消订阅
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition(topic, partition);
        self.consumer
            .incremental_unassign(&tpl)
            .expect("增量取消订阅失败");
    }

    async fn read_loop(self: Arc<Self>) {
        let mut stream = self.consumer.stream();
        while let Some(msg) = stream.next().await {
            if let Ok(m) = msg {
                let topic = m.topic().to_string();
                let partition = m.partition();
                let record = ConsumerRecord {
                    topic: topic.clone(),
                    partition,
                    offset: m.offset(),
                    key: m.key().map(|k| k.to_vec()),
                    payload: m.payload().map(|p| p.to_vec()),
                };
                let senders = self.split_senders.read().await;
                if let Some(sender) = senders.get(&(topic, partition)) {
                    let _ = sender.send(record).await;
                }
            }
        }
    }

    /// 获取 watermark
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
