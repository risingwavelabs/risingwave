use crate::error::{ErrorCode, Result, RwError};
use crate::task::{TaskManager, TaskSinkId};
use risingwave_pb::task_service::exchange_service_server::ExchangeService;
use risingwave_pb::task_service::{TaskData, TaskSinkId as ProtoTaskSinkId};
use risingwave_pb::ToProto;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ExchangeServiceImpl {
    mgr: Arc<TaskManager>,
}

impl ExchangeServiceImpl {
    pub fn new(mgr: Arc<TaskManager>) -> Self {
        ExchangeServiceImpl { mgr }
    }
}

type ExchangeDataStream = ReceiverStream<std::result::Result<TaskData, Status>>;
type ExchangeDataSender = tokio::sync::mpsc::Sender<std::result::Result<TaskData, Status>>;

#[async_trait::async_trait]
impl ExchangeService for ExchangeServiceImpl {
    type GetDataStream = ExchangeDataStream;

    #[cfg(not(tarpaulin_include))]
    async fn get_data(
        &self,
        request: Request<ProtoTaskSinkId>,
    ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("connection unestablished"))?;
        let task_sink_id = request.into_inner();
        match self.get_data_impl(peer_addr, task_sink_id).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!("Failed to serve exchange RPC from {}: {}", peer_addr, e);
                Err(e.to_grpc_status())
            }
        }
    }
}

impl ExchangeServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn get_data_impl(
        &self,
        peer_addr: SocketAddr,
        pb_tsid: ProtoTaskSinkId,
    ) -> Result<Response<<Self as ExchangeService>::GetDataStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let proto_tsid = pb_tsid.to_proto();
        let tsid = TaskSinkId::from(&proto_tsid);
        debug!("Serve exchange RPC from {} [{:?}]", peer_addr, tsid);

        let mut task_sink = self.mgr.take_sink(&proto_tsid)?;
        let mut writer = GrpcExchangeWriter::new(tx);
        task_sink.take_data(&mut writer).await?;

        info!(
            "Exchanged {} chunks from sink {:?}",
            writer.written_chunks(),
            tsid,
        );

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[async_trait::async_trait]
pub trait ExchangeWriter: Send {
    async fn write(&mut self, data: TaskData) -> Result<()>;
}

pub struct GrpcExchangeWriter {
    sender: ExchangeDataSender,
    written_chunks: usize,
}

impl GrpcExchangeWriter {
    fn new(sender: ExchangeDataSender) -> Self {
        Self {
            sender,
            written_chunks: 0,
        }
    }

    fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

#[async_trait::async_trait]
impl ExchangeWriter for GrpcExchangeWriter {
    async fn write(&mut self, data: TaskData) -> Result<()> {
        self.written_chunks += 1;
        self.sender.send(Ok(data)).await.map_err(|e| {
            RwError::from(ErrorCode::InternalError(format!(
                "failed to write data to ExchangeWriter: {}",
                e
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc::service::exchange_service::{ExchangeWriter, GrpcExchangeWriter};
    use risingwave_pb::task_service::TaskData;

    #[tokio::test]
    async fn test_exchange_writer() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let mut writer = GrpcExchangeWriter::new(tx);
        writer.write(TaskData::default()).await.unwrap();
        assert_eq!(writer.written_chunks(), 1);
    }
}
