use crate::error::{ErrorCode, Result, RwError};
use crate::task::TaskManager;
use crate::task::{TaskSink, TaskSinkId};
use futures::SinkExt;
use grpcio::{RpcContext, RpcStatus, ServerStreamingSink, WriteFlags};
use risingwave_proto::task_service::{TaskData, TaskSinkId as ProtoTaskSinkId};
use risingwave_proto::task_service_grpc::ExchangeService;
use std::sync::Arc;

#[derive(Clone)]
pub struct ExchangeServiceImpl {
    mgr: Arc<TaskManager>,
}

impl ExchangeServiceImpl {
    pub fn new(mgr: Arc<TaskManager>) -> Self {
        ExchangeServiceImpl { mgr }
    }
}

async fn pull_from_task_sink(
    task_sink_res: Result<TaskSink>,
    writer: &mut GrpcExchangeWriter<'_>,
) -> Result<()> {
    let mut task_sink = task_sink_res?;
    task_sink.take_data(writer).await?;
    Ok(())
}

impl ExchangeService for ExchangeServiceImpl {
    fn get_data(
        &mut self,
        ctx: RpcContext<'_>,
        proto_tsid: ProtoTaskSinkId,
        mut sink: ServerStreamingSink<TaskData>,
    ) {
        let tsid = TaskSinkId::from(&proto_tsid);
        let peer = ctx.peer();
        let task_sink_result = self.mgr.take_sink(&proto_tsid);
        ctx.spawn(async move {
            debug!("Serve exchange RPC from {} [{:?}]", peer, tsid);
            let mut writer = GrpcExchangeWriter::new(&mut sink);
            let res = pull_from_task_sink(task_sink_result, &mut writer)
                .await
                .map_err(|e| e.to_grpc_error());
            match res {
                Err(e) => {
                    // Augment error with TaskSinkId.
                    let ne =
                        RpcStatus::with_message(e.code(), format!("{} [{:?}]", e.message(), tsid));
                    error!("Failed to serve exchange RPC from {}: {}", peer, ne);
                    if let Err(io_err) = sink.fail(ne).await {
                        error!("Failed to fail RPC: {}", io_err);
                    }
                }
                Ok(()) => {
                    info!(
                        "Exchanged {} chunks from sink {:?}",
                        writer.written_chunks(),
                        tsid,
                    );
                    if let Err(io_err) = sink.close().await {
                        error!("Failed to close sink: {}", io_err);
                    }
                }
            };
        })
    }
}

#[async_trait::async_trait]
pub trait ExchangeWriter: Send {
    async fn write(&mut self, data: TaskData) -> Result<()>;
}

pub struct GrpcExchangeWriter<'a> {
    sink: &'a mut ServerStreamingSink<TaskData>,
    written_chunks: usize,
}

impl<'a> GrpcExchangeWriter<'a> {
    fn new(sink: &'a mut ServerStreamingSink<TaskData>) -> Self {
        Self {
            sink,
            written_chunks: 0,
        }
    }

    fn written_chunks(&self) -> usize {
        self.written_chunks
    }
}

#[async_trait::async_trait]
impl<'a> ExchangeWriter for GrpcExchangeWriter<'a> {
    async fn write(&mut self, data: TaskData) -> Result<()> {
        self.written_chunks += 1;
        self.sink
            .send((data, WriteFlags::default()))
            .await
            .map_err(|e| {
                RwError::from(ErrorCode::GrpcError(
                    "failed to send TaskData".to_string(),
                    e,
                ))
            })
    }
}
