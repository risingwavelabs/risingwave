use std::collections::HashMap;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::task_service::{CreateTaskRequest, CreateTaskResponse};
use risingwave_rpc_client::ComputeClient;
use risingwave_sqlparser::ast::ColumnOption::Default;
use crate::scheduler::plan_fragmenter::QueryStage;
use crate::scheduler::schedule::{AugmentedStage, ScheduledStage, TaskId};

pub struct TaskManager {
    compute_client_mgr: ComputeClientManager
}

impl TaskManager {
    pub fn execute(task: QueryTask) -> WorkerNode {
        let worker_node = task.stage.assignments.get(&task.task_id).unwrap();

    }

    pub fn send_create_task_request(&self, task: QueryTask, node: WorkerNode) -> CreateTaskResponse {
        // todo!()
        let mut client = self.compute_client_mgr.get_or_create(node.clone());
        let req = CreateTaskRequest {
            ..Default::default()
        };
        client.task_service.create_task(req)
    }
}

pub struct ComputeClientManager {
    clients: HashMap<WorkerNode, ComputeClient>
}

impl ComputeClientManager {
    pub fn get_or_create(&self, node: WorkerNode) -> ComputeClient {
        todo!()
    }
}

pub struct QueryTask {
    task_id: TaskId,
    stage: ScheduledStage,
    epoch: i64
}