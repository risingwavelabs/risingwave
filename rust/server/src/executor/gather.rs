use crate::error::Result;
use crate::error::RwError;
use crate::executor::{BoxedExecutor, Executor, ExecutorBuilder, ExecutorResult};
use risingwave_proto::plan::PlanNode_PlanNodeType;
use std::convert::TryFrom;

pub(super) struct GatherExecutor {
    child: BoxedExecutor,
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for GatherExecutor {
    type Error = RwError;

    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::GATHER);
        ensure!(source.plan_node().get_children().len() == 1);
        let child_plan = source.plan_node().get_children().get(0).unwrap();
        let child = ExecutorBuilder::new(child_plan, source.global_task_env().clone()).build()?;
        Ok(Self { child })
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
