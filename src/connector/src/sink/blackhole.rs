use async_trait::async_trait;
use risingwave_rpc_client::ConnectorClient;

use crate::sink::log_store::{LogReader, LogStoreReadItem};
use crate::sink::{DummySinkCommitCoordinator, LogSinker, Result, Sink, SinkWriterParam};

pub const BLACKHOLE_SINK: &str = "blackhole";

#[derive(Debug)]
pub struct BlackHoleSink;

#[async_trait]
impl Sink for BlackHoleSink {
    type Coordinator = DummySinkCommitCoordinator;
    type LogSinker = Self;

    async fn new_log_sinker(&self, _writer_env: SinkWriterParam) -> Result<Self::LogSinker> {
        Ok(Self)
    }

    async fn validate(&self, _client: Option<ConnectorClient>) -> Result<()> {
        Ok(())
    }
}

impl LogSinker for BlackHoleSink {
    async fn consume_log_and_sink(self, mut log_reader: impl LogReader) -> Result<()> {
        log_reader.init().await?;
        loop {
            if let (
                _,
                LogStoreReadItem::Barrier {
                    is_checkpoint: true,
                },
            ) = log_reader.next_item().await?
            {
                // TODO: after we support truncate at not only checkpoint barrier, we can truncate
                // at every item
                log_reader.truncate().await?;
            }
        }
    }
}
