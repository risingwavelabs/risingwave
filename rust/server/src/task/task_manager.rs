use crate::error::{ErrorCode, Result};
use crate::task::env::GlobalTaskEnv;
use crate::task::task::{TaskExecution, TaskId};
use rayon::{ThreadPool, ThreadPoolBuilder};
use risingwave_proto::plan::PlanFragment;
use risingwave_proto::task_service::{TaskId as ProtoTaskId, TaskSinkId as ProtoSinkId};
use std::collections::HashMap;
use std::sync::Mutex;

pub(crate) struct TaskManager {
    worker_pool: ThreadPool,
    tasks: Mutex<HashMap<TaskId, Box<TaskExecution>>>,
    env: GlobalTaskEnv,
}

impl TaskManager {
    pub(crate) fn new(env: GlobalTaskEnv) -> Self {
        let worker_pool = ThreadPoolBuilder::default().num_threads(2).build().unwrap();
        TaskManager {
            worker_pool,
            tasks: Mutex::new(HashMap::new()),
            env,
        }
    }

    pub(crate) fn fire_task(&mut self, tid: &ProtoTaskId, plan: PlanFragment) -> Result<()> {
        let tsk = TaskExecution::new(tid, plan, self.env.clone());
        self.tasks
            .lock()
            .unwrap()
            .entry(tsk.get_task_id().clone())
            .or_insert_with(|| Box::new(tsk))
            .async_execute(&mut self.worker_pool)?;
        Ok(())
    }

    // NOTE: For now, draining out data from the task will block the grpc thread.
    // By design, `take_data` is an future, but since it's unsafe to mix other
    // future runtimes (tokio, eg.) with grpc-rs reactor, we
    pub(crate) fn take_task(&mut self, sid: &ProtoSinkId) -> Result<Box<TaskExecution>> {
        let task_id = TaskId::from(sid.get_task_id());
        let tsk = match self.tasks.lock().unwrap().remove(&task_id) {
            Some(t) => t,
            None => {
                return Err(
                    ErrorCode::InternalError(format!("No such task {:?}", &task_id)).into(),
                );
            }
        };
        Ok(tsk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::ExchangeWriter;
    use protobuf::Message;
    use risingwave_proto::plan::{CreateTableNode, PlanNode_PlanNodeType as PlanNodeType};
    use risingwave_proto::task_service::{TaskData, TaskSinkId};

    struct FakeExchangeWriter {
        messages: Vec<TaskData>,
    }

    #[async_trait::async_trait]
    impl ExchangeWriter for FakeExchangeWriter {
        async fn write(&mut self, data: TaskData) -> Result<()> {
            self.messages.push(data);
            Ok(())
        }
    }

    #[test]
    fn test_task_manager() -> Result<()> {
        let mut mgr = TaskManager::new(GlobalTaskEnv::for_test());
        let tid = ProtoTaskId::default();
        let mut plan = PlanFragment::default();
        let create = CreateTableNode::default();
        plan.mut_root()
            .mut_body()
            .merge_from_bytes(create.write_to_bytes().unwrap().as_slice())
            .unwrap();
        plan.mut_root().set_node_type(PlanNodeType::CREATE_TABLE);
        mgr.fire_task(&tid, plan)?;

        let mut tsid = TaskSinkId::default();
        tsid.set_task_id(tid.clone());
        let mut task = mgr.take_task(&tsid)?;

        let mut writer = FakeExchangeWriter { messages: vec![] };
        let res = futures::executor::block_on(async move { task.take_data(0, &mut writer).await });
        assert!(res.is_err());
        // TODO: Let this test succeed.
        Ok(())
    }
}
