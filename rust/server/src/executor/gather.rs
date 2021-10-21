use crate::error::Result;
use crate::executor::{BoxedExecutor, Executor, ExecutorBuilder, ExecutorResult};
use risingwave_proto::plan::PlanNode_PlanNodeType;

use super::BoxedExecutorBuilder;

pub(super) struct GatherExecutor {
    child: BoxedExecutor,
}

impl BoxedExecutorBuilder for GatherExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::GATHER);
        ensure!(source.plan_node().get_children().len() == 1);
        let child_plan = source.plan_node().get_children().get(0).unwrap();
        let child = ExecutorBuilder::new(child_plan, source.global_task_env().clone()).build()?;
        Ok(Box::new(Self { child }))
    }
}

#[async_trait::async_trait]
impl Executor for GatherExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        self.child.execute()
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()
    }
}
