use crate::task::TaskManager;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, UnarySink};
use risingwave_proto::task_service::{
    AbortTaskRequest, AbortTaskResponse, CreateTaskRequest, CreateTaskResponse, GetTaskInfoRequest,
    GetTaskInfoResponse,
};
use risingwave_proto::task_service_grpc::TaskService;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct TaskServiceImpl {
    mgr: Arc<Mutex<TaskManager>>,
}

impl TaskServiceImpl {
    pub fn new(mgr: Arc<Mutex<TaskManager>>) -> Self {
        TaskServiceImpl { mgr }
    }
}

impl TaskService for TaskServiceImpl {
    fn create(
        &mut self,
        _: RpcContext<'_>,
        req: CreateTaskRequest,
        resp_sink: UnarySink<CreateTaskResponse>,
    ) {
        // NOTE: Due to the limitation of the generated grpc api, we cannot borrow the mutable data inside.
        // So here, we have to accept this copy overhead.
        let plan = req.get_plan().clone();
        let mut mgr = self.mgr.lock().unwrap();
        match mgr.fire_task(req.get_task_id(), plan) {
            Err(e) => resp_sink.fail(RpcStatus::with_message(
                RpcStatusCode::INTERNAL,
                e.to_string(),
            )),
            Ok(()) => resp_sink.success(CreateTaskResponse::default()),
        };
    }

    fn get_task_info(
        &mut self,
        _: RpcContext<'_>,
        _: GetTaskInfoRequest,
        _: UnarySink<GetTaskInfoResponse>,
    ) {
        todo!()
    }

    fn abort(&mut self, _: RpcContext<'_>, _: AbortTaskRequest, _: UnarySink<AbortTaskResponse>) {
        todo!()
    }
}
