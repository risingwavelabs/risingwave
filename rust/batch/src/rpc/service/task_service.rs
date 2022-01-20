use std::sync::Arc;

use risingwave_pb::task_service::task_service_server::TaskService;
use risingwave_pb::task_service::{
    AbortTaskRequest, AbortTaskResponse, CreateTaskRequest, CreateTaskResponse, GetTaskInfoRequest,
    GetTaskInfoResponse,
};
use tonic::{Request, Response, Status};

use crate::task::{BatchTaskEnv, TaskManager};

#[derive(Clone)]
pub struct TaskServiceImpl {
    mgr: Arc<TaskManager>,
    env: BatchTaskEnv,
}

impl TaskServiceImpl {
    pub fn new(mgr: Arc<TaskManager>, env: BatchTaskEnv) -> Self {
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

        let res = self.mgr.fire_task(
            self.env.clone(),
            req.get_task_id().expect("no task id found"),
            req.get_plan().expect("no plan found").clone(),
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
