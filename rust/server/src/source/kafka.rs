use risingwave_common::error::{Result, RwError};

use rdkafka::consumer::{Consumer, DefaultConsumerContext, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message};
use std::collections::HashMap;
use std::fmt::Debug;

use crate::source::{Source, SourceConfig, SourceMessage, SourceReader};
use chrono::Local;
use rdkafka::util::AsyncRuntime;
use risingwave_common::error::ErrorCode::InternalError;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct KafkaSourceConfig {
    pub bootstrap_servers: Vec<String>,
    pub topic: String,
    pub properties: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct KafkaSource {
    config: KafkaSourceConfig,
}

impl Source for KafkaSource {
    fn new(config: SourceConfig) -> Result<Self>
    where
        Self: Sized,
    {
        match config {
            SourceConfig::Kafka(kafka_config) => Ok(KafkaSource {
                config: kafka_config,
            }),
            _ => Err(RwError::from(InternalError(
                "config is not kafka config".into(),
            ))),
        }
    }

    fn reader(&self) -> Result<Box<dyn SourceReader>> {
        let mut client_config: ClientConfig = ClientConfig::new();

        client_config.set("bootstrap.servers", self.config.bootstrap_servers.join(","));

        // todo, using random string
        client_config.set("group.id", format!("consumer-{}", Local::now().timestamp()));

        client_config.set("enable.auto.commit", "false");
        client_config.set("enable.partition.eof", "true");
        client_config.set("auto.offset.reset", "earliest");
        client_config.set("topic.metadata.refresh.interval.ms", "30000"); // 30 seconds
        client_config.set("fetch.message.max.bytes", "134217728");

        for (k, v) in &self.config.properties {
            client_config.set(k, v);
        }

        let consumer: StreamConsumer<DefaultConsumerContext, AsyncStdRuntime> = client_config
            .create_with_context(DefaultConsumerContext)
            .map_err(|e| RwError::from(InternalError(e.to_string())))?;

        let topics: &[&str] = &[&self.config.topic];

        consumer
            .subscribe(&topics.to_vec())
            .map_err(|e| RwError::from(InternalError(e.to_string())))?;

        Ok(Box::new(KafkaSourceReader {
            consumer,
            kafka_source: self.clone(),
        }))
    }
}

#[derive(Clone, Debug)]
pub struct KafkaMessage {
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Option<Vec<u8>>,
}

pub struct KafkaSourceReader {
    consumer: StreamConsumer<DefaultConsumerContext, AsyncStdRuntime>,
    kafka_source: KafkaSource,
}

impl std::fmt::Debug for KafkaSourceReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSourceReader")
            .field("kafka_source", &self.kafka_source)
            .finish()
    }
}

pub struct AsyncStdRuntime;

impl AsyncRuntime for AsyncStdRuntime {
    type Delay = Pin<Box<dyn Future<Output = ()> + Send>>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        async_std::task::spawn(task);
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        Box::pin(async_std::task::sleep(duration))
    }
}

#[async_trait::async_trait]
impl SourceReader for KafkaSourceReader {
    async fn next(&mut self) -> Result<Option<SourceMessage>> {
        match self.consumer.recv().await {
            Err(e) => match e {
                KafkaError::PartitionEOF(_) => Ok(None),
                _ => return Err(RwError::from(InternalError(e.to_string()))),
            },

            Ok(m) => {
                let payload = m.payload_view::<[u8]>().map(|x| x.unwrap());
                Ok(Some(SourceMessage::Kafka(KafkaMessage {
                    partition: m.partition(),
                    offset: m.offset(),
                    key: m.key().map(|v| v.to_vec()),
                    payload: payload.map(|x| x.to_vec()),
                })))
            }
        }
    }

    async fn cancel(&mut self) -> Result<()> {
        self.consumer.unsubscribe();
        Ok(())
    }
}
