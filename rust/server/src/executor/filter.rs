use super::BoxedExecutor;
use crate::array2::ArrayImpl::Bool;
use crate::buffer::Bitmap;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::executor::ExecutorResult::{Batch, Done};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{build_from_proto, BoxedExpression};
use protobuf::Message;
use risingwave_proto::plan::{FilterNode, PlanNode_PlanNodeType};
use std::convert::TryFrom;
use std::sync::Arc;
pub(super) struct FilterExecutor {
    expr: BoxedExpression,
    child: BoxedExecutor,
}

impl Executor for FilterExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        let res = self.child.execute()?;
        if let Batch(data_chunk) = res {
            let vis_array = self.expr.eval(data_chunk.as_ref())?;
            if let Bool(vis) = vis_array.as_ref() {
                let mut vis = Bitmap::from_bool_array(vis)?;
                if let Some(old_vis) = data_chunk.visibility() {
                    vis = ((&vis) & old_vis)?;
                }
                let data_chunk = data_chunk.with_visibility(vis);
                return data_chunk.compact().map(|x| Batch(Arc::new(x)));
            } else {
                return Err(InternalError("Filter can only receive bool array".to_string()).into());
            }
        }
        Ok(Done)
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()
    }
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for FilterExecutor {
    type Error = RwError;
    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::FILTER);
        ensure!(source.plan_node().get_children().len() == 1);
        let filter_node = FilterNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let expr_node = filter_node.get_search_condition();
        let expr = build_from_proto(expr_node)?;
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child =
                ExecutorBuilder::new(child_plan, source.global_task_env().clone()).build()?;
            return Ok(Self { expr, child });
        }
        Err(InternalError("Filter must have one children".to_string()).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::column::Column;
    use crate::array2::{Array, DataChunk, PrimitiveArray};
    use crate::executor::test_utils::MockExecutor;
    use crate::types::Int32Type;
    use pb_construct::make_proto;
    use protobuf::well_known_types::Any as AnyProto;
    use protobuf::RepeatedField;
    use risingwave_proto::data::DataType as DataTypeProto;
    use risingwave_proto::expr::ExprNode_Type::EQUAL;
    use risingwave_proto::expr::ExprNode_Type::INPUT_REF;
    use risingwave_proto::expr::FunctionCall;
    use risingwave_proto::expr::InputRefExpr;
    use risingwave_proto::expr::{ExprNode, ExprNode_Type};
    use std::sync::Arc;
    #[test]
    fn test_filter_executor() {
        let col1 = create_column(&[Some(2), Some(2)]).unwrap();
        let col2 = create_column(&[Some(1), Some(2)]).unwrap();
        let data_chunk = DataChunk::builder().columns([col1, col2].to_vec()).build();
        let mut mock_executor = MockExecutor::new();
        mock_executor.add(data_chunk);
        let expr = make_expression(EQUAL);
        let mut filter_executor = FilterExecutor {
            expr: build_from_proto(&expr).unwrap(),
            child: Box::new(mock_executor),
        };
        let res = filter_executor.execute().unwrap();
        if let Batch(res) = res {
            let col1 = res.as_ref().column_at(0).unwrap();
            let array = col1.array();
            let col1 = array.as_int32();
            assert_eq!(col1.len(), 1);
        }
    }

    fn make_expression(kind: ExprNode_Type) -> ExprNode {
        let lhs = make_inputref(0);
        let rhs = make_inputref(1);
        make_proto!(ExprNode, {
          expr_type: kind,
          body: AnyProto::pack(
            &make_proto!(FunctionCall, {
              children: RepeatedField::from_slice(&[lhs, rhs])
            })
          ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
            type_name: risingwave_proto::data::DataType_TypeName::BOOLEAN
          })
        })
    }

    fn make_inputref(idx: i32) -> ExprNode {
        make_proto!(ExprNode, {
          expr_type: INPUT_REF,
          body: AnyProto::pack(
            &make_proto!(InputRefExpr, {column_idx: idx})
          ).unwrap(),
          return_type: make_proto!(DataTypeProto, {
            type_name: risingwave_proto::data::DataType_TypeName::INT32
          })
        })
    }

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        let data_type = Arc::new(Int32Type::new(false));
        Ok(Column::new(array, data_type))
    }
}
