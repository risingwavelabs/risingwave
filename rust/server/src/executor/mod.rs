mod seqscan;

use crate::array::DataChunk;
use crate::error::Result;
use risingwave_proto::plan::PlanNode;

pub(crate) enum ExecutorResult {
  Batch(DataChunk),
  Done,
}

pub(crate) trait Executor: Send {
  fn init(&mut self) -> Result<()>;
  fn execute(&mut self) -> Result<ExecutorResult>;
  fn clean(&mut self) -> Result<()>;
}

pub(crate) type BoxedExecutor = Box<dyn Executor>;

pub(crate) fn create_executor(_plan_node: &PlanNode) -> Result<BoxedExecutor> {
  unimplemented!()
}
