use super::BoxedExecutor;
use crate::array::{ArrayRef, DataChunk};
use crate::error::ErrorCode::ProtobufError;
use crate::error::{ErrorCode, Result, RwError};
use crate::executor::ExecutorResult::{Batch, Done};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{build_from_proto, BoxedExpression};
use protobuf::Message;
use risingwave_proto::plan::{PlanNode_PlanNodeType, ProjectNode};
use std::convert::TryFrom;
use std::sync::Arc;

pub(super) struct ProjectionExecutor {
    expr: Vec<BoxedExpression>,
    child: BoxedExecutor,
}

impl Executor for ProjectionExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()?;
        println!("expr len:{}", self.expr.len());
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        let child_output = self.child.execute()?;
        match child_output {
            Batch(child_chunk) => {
                let cardinality = child_chunk.cardinality();
                let arrays: Vec<ArrayRef> = self
                    .expr
                    .iter_mut()
                    .map(|expr| expr.eval(&child_chunk))
                    .collect::<Result<Vec<ArrayRef>>>()?;
                let ret = DataChunk::builder()
                    .cardinality(cardinality)
                    .arrays(arrays)
                    .build();
                Ok(Batch(Arc::new(ret)))
            }
            Done => Ok(Done),
        }
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()?;
        Ok(())
    }
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for ProjectionExecutor {
    type Error = RwError;
    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::PROJECT);
        ensure!(source.plan_node().get_children().len() == 1);
        let proto_value = source.plan_node().get_body();
        println!("proj proto_value:{:?}", proto_value);

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

        Ok(Self {
            expr: project_exprs,
            child: child_node,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{DataChunk, PrimitiveArray};
    use crate::error::ErrorCode::InternalError;
    use crate::error::Result;
    use crate::error::RwError;
    use crate::executor::test_utils::MockExecutor;
    use crate::expr::new_input_ref;
    use crate::types::Int32Type;
    use crate::util::downcast_ref;
    use std::sync::Arc;

    #[test]
    fn test_project_executor() -> Result<()> {
        let arr_1 = PrimitiveArray::<Int32Type>::from_slice(vec![1, 2, 33333, 4, 5])?;
        let arr_2 = PrimitiveArray::<Int32Type>::from_slice(vec![7, 8, 66666, 4, 3])?;
        let arr_3 = PrimitiveArray::<Int32Type>::from_slice(vec![4, 2, 11111, 1, 9])?;

        let arrays = vec![arr_1, arr_2, arr_3];

        let chunk = DataChunk::builder().cardinality(5).arrays(arrays).build();

        let type1 = Arc::new(Int32Type::new(false));
        let expr1 = new_input_ref(type1, 0);
        let expr_vec = vec![expr1];

        let mut mock_executor = MockExecutor::new();
        mock_executor.add(chunk);

        let mut proj_executor = ProjectionExecutor {
            expr: expr_vec,
            child: Box::new(mock_executor),
        };

        assert!(proj_executor.init().is_ok());

        let result = proj_executor.execute()?;
        match result {
            Batch(result_chunk) => {
                assert_eq!(result_chunk.dimension(), 1);
                let result_arr = result_chunk.array_at(0)?;
                assert_eq!(result_arr.len(), 5);

                let result_ref: &PrimitiveArray<Int32Type> = downcast_ref(&*result_arr)?;
                let result_slice = result_ref.as_slice();
                let expected_vec = vec![1, 2, 33333, 4, 5];
                for _i in 0..5 {
                    assert_eq!(result_slice[_i], expected_vec[_i]);
                }
                Ok(())
            }
            Done => Err(RwError::from(InternalError("Incorrect answer".to_string()))),
        }
    }
}
