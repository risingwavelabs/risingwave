use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::task_service::GetDataResponse;
use tonic::Status;

type ExchangeDataSender = tokio::sync::mpsc::Sender<std::result::Result<GetDataResponse, Status>>;

#[async_trait::async_trait]
pub trait ExchangeWriter: Send {
    async fn write(&mut self, resp: GetDataResponse) -> Result<()>;
}

pub struct GrpcExchangeWriter {
    sender: ExchangeDataSender,
    written_chunks: usize,
}

impl GrpcExchangeWriter {
    pub fn new(sender: ExchangeDataSender) -> Self {
        Self {
            sender,
            written_chunks: 0,
        }
    }

    pub fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

#[async_trait::async_trait]
impl ExchangeWriter for GrpcExchangeWriter {
    async fn write(&mut self, data: GetDataResponse) -> Result<()> {
        self.written_chunks += 1;
        self.sender
            .send(Ok(data))
            .await
            .to_rw_result_with("failed to write data to ExchangeWriter")
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::task_service::GetDataResponse;

    use crate::rpc::service::exchange::{ExchangeWriter, GrpcExchangeWriter};

    #[tokio::test]
    async fn test_exchange_writer() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let mut writer = GrpcExchangeWriter::new(tx);
        writer.write(GetDataResponse::default()).await.unwrap();
        assert_eq!(writer.written_chunks(), 1);
    }

    #[tokio::test]
    async fn test_write_to_closed_channel() {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        drop(rx);
        let mut writer = GrpcExchangeWriter::new(tx);
        let res = writer.write(GetDataResponse::default()).await;
        assert!(res.is_err());
    }
}
