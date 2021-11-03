use crate::task::{GlobalTaskEnv, TaskManager};
use risingwave_pb::task_service::task_service_server::TaskService;
use risingwave_pb::task_service::{
    AbortTaskRequest, AbortTaskResponse, CreateTaskRequest, CreateTaskResponse, GetTaskInfoRequest,
    GetTaskInfoResponse,
};
use risingwave_pb::ToProto;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct TaskServiceImpl {
    mgr: Arc<TaskManager>,
    env: GlobalTaskEnv,
}

impl TaskServiceImpl {
    pub fn new(mgr: Arc<TaskManager>, env: GlobalTaskEnv) -> Self {
        TaskServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl TaskService for TaskServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn create_task(
        &self,
        request: Request<CreateTaskRequest>,
    ) -> Result<Response<CreateTaskResponse>, Status> {
        let req = request.into_inner();

        let plan = req.get_plan().to_proto();
        let res = self.mgr.fire_task(
            self.env.clone(),
            &req.get_task_id()
                .to_proto::<risingwave_proto::task_service::TaskId>(),
            plan,
        );
        match res {
            Ok(_) => Ok(Response::new(CreateTaskResponse { status: None })),
            Err(e) => {
                error!("failed to fire task {}", e);
                Err(e.to_grpc_status())
            }
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn get_task_info(
        &self,
        _: Request<GetTaskInfoRequest>,
    ) -> Result<Response<GetTaskInfoResponse>, Status> {
        todo!()
    }

    #[cfg(not(tarpaulin_include))]
    async fn abort_task(
        &self,
        _: Request<AbortTaskRequest>,
    ) -> Result<Response<AbortTaskResponse>, Status> {
        todo!()
    }
}
