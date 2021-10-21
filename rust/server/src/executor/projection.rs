use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::array::column::Column;
use crate::array::DataChunk;
use crate::error::ErrorCode::ProtobufError;
use crate::error::{ErrorCode, Result, RwError};
use crate::executor::ExecutorResult::{Batch, Done};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{build_from_proto, BoxedExpression};
use protobuf::Message;
use risingwave_proto::plan::{PlanNode_PlanNodeType, ProjectNode};

pub(super) struct ProjectionExecutor {
    expr: Vec<BoxedExpression>,
    child: BoxedExecutor,
}

impl Executor for ProjectionExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        let child_output = self.child.execute()?;
        match child_output {
            Batch(child_chunk) => {
                let arrays: Vec<Column> = self
                    .expr
                    .iter_mut()
                    .map(|expr| {
                        expr.eval(&child_chunk)
                            .map(|arr| Column::new(arr, expr.return_type_ref()))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let ret = DataChunk::builder().columns(arrays).build();
                Ok(Batch(ret))
            }
            Done => Ok(Done),
        }
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()?;
        Ok(())
    }
}

impl BoxedExecutorBuilder for ProjectionExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::PROJECT);
        ensure!(source.plan_node().get_children().len() == 1);

        let project_node = ProjectNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child_node =
            ExecutorBuilder::new(proto_child, source.global_task_env().clone()).build()?;

        let project_exprs = project_node
            .get_select_list()
            .iter()
            .map(build_from_proto)
            .collect::<Result<Vec<BoxedExpression>>>()?;

        Ok(Box::new(Self {
            expr: project_exprs,
            child: child_node,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{Array, I32Array};
    use crate::executor::test_utils::MockExecutor;
    use crate::expr::InputRefExpression;
    use crate::types::Int32Type;
    use crate::*;

    #[test]
    fn test_project_executor() -> Result<()> {
        let col1 = column_nonnull! {I32Array, Int32Type, [1, 2, 33333, 4, 5]};
        let col2 = column_nonnull! {I32Array, Int32Type, [7, 8, 66666, 4, 3]};
        let chunk = DataChunk::builder().columns(vec![col1, col2]).build();

        let type1 = Int32Type::create(false);
        let expr1 = InputRefExpression::new(type1, 0);
        let expr_vec = vec![Box::new(expr1) as BoxedExpression];

        let mut mock_executor = MockExecutor::new();
        mock_executor.add(chunk);

        let mut proj_executor = ProjectionExecutor {
            expr: expr_vec,
            child: Box::new(mock_executor),
        };
        assert!(proj_executor.init().is_ok());

        let result_chunk = proj_executor.execute()?.batch_or()?;
        assert!(proj_executor.clean().is_ok());
        assert_eq!(result_chunk.dimension(), 1);
        assert_eq!(
            result_chunk
                .column_at(0)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(33333), Some(4), Some(5)]
        );

        Ok(())
    }
}
