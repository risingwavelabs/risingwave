use std::net::SocketAddr;
use std::sync::Arc;

use futures::channel::mpsc::Receiver;
use futures::StreamExt;
use risingwave_batch::rpc::service::exchange::GrpcExchangeWriter;
use risingwave_batch::task::{BatchManager, TaskSinkId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan::TaskSinkId as ProtoTaskSinkId;
use risingwave_pb::task_service::exchange_service_server::ExchangeService;
use risingwave_pb::task_service::{
    GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
};
use risingwave_stream::executor::Message;
use risingwave_stream::task::StreamManager;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct ExchangeServiceImpl {
    batch_mgr: Arc<BatchManager>,
    stream_mgr: Arc<StreamManager>,
}

type ExchangeDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;

#[async_trait::async_trait]
impl ExchangeService for ExchangeServiceImpl {
    type GetDataStream = ExchangeDataStream;
    type GetStreamStream = ReceiverStream<std::result::Result<GetStreamResponse, Status>>;

    #[cfg(not(tarpaulin_include))]
    async fn get_data(
        &self,
        request: Request<GetDataRequest>,
    ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
        use risingwave_common::error::tonic_err;

        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("connection unestablished"))?;
        let req = request.into_inner();
        match self
            .get_data_impl(peer_addr, req.get_sink_id().map_err(tonic_err)?.clone())
            .await
        {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!("Failed to serve exchange RPC from {}: {}", peer_addr, e);
                Err(e.to_grpc_status())
            }
        }
    }

    async fn get_stream(
        &self,
        request: Request<GetStreamRequest>,
    ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
        let peer_addr = request
            .remote_addr()
            .ok_or_else(|| Status::unavailable("get_stream connection unestablished"))?;
        let req = request.into_inner();
        let up_down_ids = (req.up_fragment_id, req.down_fragment_id);
        let receiver = self
            .stream_mgr
            .take_receiver(up_down_ids)
            .map_err(|e| e.to_grpc_status())?;
        match self.get_stream_impl(peer_addr, receiver).await {
            Ok(resp) => Ok(resp),
            Err(e) => {
                error!(
                    "Failed to server stream exchange RPC from {}: {}",
                    peer_addr, e
                );
                Err(e.to_grpc_status())
            }
        }
    }
}

impl ExchangeServiceImpl {
    pub fn new(mgr: Arc<BatchManager>, stream_mgr: Arc<StreamManager>) -> Self {
        ExchangeServiceImpl {
            batch_mgr: mgr,
            stream_mgr,
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_data_impl(
        &self,
        peer_addr: SocketAddr,
        pb_tsid: ProtoTaskSinkId,
    ) -> Result<Response<<Self as ExchangeService>::GetDataStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);

        let tsid = TaskSinkId::try_from(&pb_tsid)?;
        debug!("Serve exchange RPC from {} [{:?}]", peer_addr, tsid);
        let mut task_sink = self.batch_mgr.take_sink(&pb_tsid)?;
        tokio::spawn(async move {
            let mut writer = GrpcExchangeWriter::new(tx.clone());
            match task_sink.take_data(&mut writer).await {
                Ok(_) => {
                    info!(
                        "Exchanged {} chunks from sink {:?}",
                        writer.written_chunks(),
                        tsid,
                    );
                    Ok(())
                }
                Err(e) => tx.send(Err(e.to_grpc_status())).await,
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_stream_impl(
        &self,
        peer_addr: SocketAddr,
        mut receiver: Receiver<Message>,
    ) -> Result<Response<<Self as ExchangeService>::GetStreamStream>> {
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        debug!("Serve stream exchange RPC from {}", peer_addr);
        tokio::spawn(async move {
            loop {
                let msg = receiver.next().await;
                match msg {
                    // the sender is closed, we close the receiver and stop forwarding message
                    None => break,
                    Some(msg) => {
                        let res = match msg.to_protobuf() {
                            Ok(stream_msg) => Ok(GetStreamResponse {
                                message: Some(stream_msg),
                            }),
                            Err(e) => Err(e.to_grpc_status()),
                        };
                        let _ = match tx.send(res).await.map_err(|e| {
                            RwError::from(ErrorCode::InternalError(format!(
                                "failed to send stream data: {}",
                                e
                            )))
                        }) {
                            Ok(_) => Ok(()),
                            Err(e) => tx.send(Err(e.to_grpc_status())).await,
                        };
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
