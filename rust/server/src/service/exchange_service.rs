use crate::error::Result;
use crate::task::{TaskExecution, TaskManager};
use futures::SinkExt;
use grpcio::{RpcContext, ServerStreamingSink};
use risingwave_proto::task_service::{TaskData, TaskSinkId};
use risingwave_proto::task_service_grpc::ExchangeService;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct ExchangeServiceImpl {
    mgr: Arc<Mutex<TaskManager>>,
}

impl ExchangeServiceImpl {
    pub fn new(mgr: Arc<Mutex<TaskManager>>) -> Self {
        ExchangeServiceImpl { mgr }
    }
}

async fn pull_from_task(
    tsk_res: Result<Box<TaskExecution>>,
    tsid: &TaskSinkId,
    mut sink: &mut ServerStreamingSink<TaskData>,
) -> Result<()> {
    let mut tsk = tsk_res?;
    tsk.take_data(tsid.get_sink_id(), &mut sink).await?;
    Ok(())
}

impl ExchangeService for ExchangeServiceImpl {
    fn get_data(
        &mut self,
        ctx: RpcContext<'_>,
        tsid: TaskSinkId,
        mut sink: ServerStreamingSink<TaskData>,
    ) {
        let task_result = self.mgr.lock().unwrap().take_task(&tsid);
        ctx.spawn(async move {
            let res = pull_from_task(task_result, &tsid, &mut sink)
                .await
                .map_err(|e| e.to_grpc_error());
            match res {
                Err(e) => {
                    sink.fail(e).await.unwrap();
                }
                Ok(()) => {
                    sink.close().await.unwrap();
                }
            };
        })
    }
}
