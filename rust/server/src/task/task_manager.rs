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
    use protobuf::well_known_types::Any;
    use risingwave_proto::data::{DataType, DataType_TypeName};
    use risingwave_proto::expr::{ConstantValue, ExprNode, ExprNode_ExprNodeType};
    use risingwave_proto::plan::{
        ColumnDesc, CreateTableNode, InsertValueNode, InsertValueNode_ExprTuple,
        PlanNode_PlanNodeType as PlanNodeType,
    };
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

    // create table t(v1 int not null);
    fn create_table_v1_int32() -> PlanFragment {
        let mut plan = PlanFragment::default();

        let mut create = CreateTableNode::default();
        let mut typ = DataType::new();
        typ.set_type_name(DataType_TypeName::INT32);
        let mut col = ColumnDesc::default();
        col.set_column_type(typ);
        create.mut_column_descs().push(col);

        plan.mut_root().set_body(Any::pack(&create).unwrap());
        plan.mut_root().set_node_type(PlanNodeType::CREATE_TABLE);
        plan
    }

    // insert into t values(1);
    fn insert_values_v1() -> PlanFragment {
        let mut plan = PlanFragment::default();

        let mut insert = InsertValueNode::default();
        insert.mut_column_ids().push(0);
        let mut tuple = InsertValueNode_ExprTuple::default();
        let mut cell = ExprNode::default();
        cell.set_expr_type(ExprNode_ExprNodeType::CONSTANT_VALUE);
        let mut typ = DataType::new();
        typ.set_type_name(DataType_TypeName::INT32);
        cell.set_return_type(typ);
        let mut constant = ConstantValue::default();
        constant.set_body(Vec::from(1i32.to_be_bytes()));
        cell.set_body(Any::pack(&constant).unwrap());
        tuple.mut_cells().push(cell);
        insert.insert_tuples.push(tuple);

        plan.mut_root().set_body(Any::pack(&insert).unwrap());
        plan.mut_root().set_node_type(PlanNodeType::INSERT_VALUE);
        plan
    }

    #[test]
    fn test_task_manager() -> Result<()> {
        let mut mgr = TaskManager::new(GlobalTaskEnv::for_test());
        let tid = ProtoTaskId::default();
        let mut tsid = TaskSinkId::default();
        tsid.set_task_id(tid.clone());
        {
            mgr.fire_task(&tid, create_table_v1_int32())?;
            let mut task = mgr.take_task(&tsid)?;
            let mut writer = FakeExchangeWriter { messages: vec![] };
            futures::executor::block_on(async move { task.take_data(0, &mut writer).await })?;
        }
        {
            mgr.fire_task(&tid, insert_values_v1())?;
            let mut task = mgr.take_task(&tsid)?;
            let mut writer = FakeExchangeWriter { messages: vec![] };
            futures::executor::block_on(async move { task.take_data(0, &mut writer).await })?;
        }
        Ok(())
    }
}
