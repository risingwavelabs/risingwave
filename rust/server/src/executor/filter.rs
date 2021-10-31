use protobuf::Message;

use risingwave_proto::plan::{FilterNode, PlanNode_PlanNodeType};

use crate::executor::ExecutorResult::{Batch, Done};
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::{InternalError, ProtobufError};
use risingwave_common::error::Result;
use risingwave_common::expr::{build_from_proto, BoxedExpression};

use super::{BoxedExecutor, BoxedExecutorBuilder};

pub(super) struct FilterExecutor {
    expr: BoxedExpression,
    child: BoxedExecutor,
}

#[async_trait::async_trait]
impl Executor for FilterExecutor {
    fn init(&mut self) -> Result<()> {
        self.child.init()
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        let res = self.child.execute().await?;
        if let Batch(data_chunk) = res {
            let vis_array = self.expr.eval(&data_chunk)?;
            if let Bool(vis) = vis_array.as_ref() {
                let mut vis = vis.try_into()?;
                if let Some(old_vis) = data_chunk.visibility() {
                    vis = ((&vis) & old_vis)?;
                }
                let data_chunk = data_chunk.with_visibility(vis);
                return data_chunk.compact().map(Batch);
            } else {
                return Err(InternalError("Filter can only receive bool array".to_string()).into());
            }
        }
        Ok(Done)
    }

    fn clean(&mut self) -> Result<()> {
        self.child.clean()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}

impl BoxedExecutorBuilder for FilterExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::FILTER);
        ensure!(source.plan_node().get_children().len() == 1);
        let filter_node = FilterNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;
        let expr_node = filter_node.get_search_condition();
        let expr = build_from_proto(expr_node)?;
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child = source.clone_for_plan(child_plan).build()?;
            return Ok(Box::new(Self { expr, child }));
        }
        Err(InternalError("Filter must have one children".to_string()).into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use protobuf::well_known_types::Any as AnyProto;
    use protobuf::RepeatedField;

    use pb_construct::make_proto;
    use risingwave_proto::data::DataType as DataTypeProto;
    use risingwave_proto::expr::ExprNode_Type::EQUAL;
    use risingwave_proto::expr::ExprNode_Type::INPUT_REF;
    use risingwave_proto::expr::FunctionCall;
    use risingwave_proto::expr::InputRefExpr;
    use risingwave_proto::expr::{ExprNode, ExprNode_Type};

    use crate::executor::test_utils::MockExecutor;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{Array, DataChunk, PrimitiveArray};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataTypeKind, Int32Type};

    use super::*;

    #[tokio::test]
    async fn test_filter_executor() {
        let col1 = create_column(&[Some(2), Some(2)]).unwrap();
        let col2 = create_column(&[Some(1), Some(2)]).unwrap();
        let data_chunk = DataChunk::builder().columns([col1, col2].to_vec()).build();
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int32Type::create(false),
                },
                Field {
                    data_type: Int32Type::create(false),
                },
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(data_chunk);
        let expr = make_expression(EQUAL);
        let mut filter_executor = FilterExecutor {
            expr: build_from_proto(&expr).unwrap(),
            child: Box::new(mock_executor),
        };
        let fields = &filter_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);
        assert_eq!(fields[1].data_type.data_type_kind(), DataTypeKind::Int32);
        let res = filter_executor.execute().await.unwrap();
        if let Batch(res) = res {
            let col1 = res.column_at(0).unwrap();
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
        let data_type = Int32Type::create(false);
        Ok(Column::new(array, data_type))
    }
}
