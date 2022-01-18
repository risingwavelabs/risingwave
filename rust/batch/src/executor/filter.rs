use prost::Message;
use risingwave_common::array::ArrayImpl::Bool;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::ErrorCode::{InternalError, ProstError};
use risingwave_common::error::Result;
use risingwave_common::expr::{build_from_prost, BoxedExpression};
use risingwave_common::util::chunk_coalesce::{
    DataChunkBuilder, SlicedDataChunk, DEFAULT_CHUNK_BUFFER_SIZE,
};
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::FilterNode;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

pub(super) struct FilterExecutor {
    expr: BoxedExpression,
    child: BoxedExecutor,
    chunk_builder: DataChunkBuilder,
    last_input: Option<SlicedDataChunk>,
    identity: String,
}

#[async_trait::async_trait]
impl Executor for FilterExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        loop {
            let tmp_last_input = self.last_input.take();

            // We have something left from last poll of child
            if let Some(existing_data_chunk) = tmp_last_input {
                let (left_data_chunk, return_data_chunk) =
                    self.chunk_builder.append_chunk(existing_data_chunk)?;
                self.last_input = left_data_chunk;

                if let Some(data_chunk) = return_data_chunk {
                    return Ok(Some(data_chunk));
                }
            } else {
                let child_input = self.fetch_one_chunk().await?;
                if let Some(data_chunk) = child_input {
                    self.last_input = Some(SlicedDataChunk::new_checked(data_chunk)?);
                } else {
                    // We should return here since nothing come from child.
                    return if let Some(left) = self.chunk_builder.consume_all()? {
                        Ok(Some(left))
                    } else {
                        Ok(None)
                    };
                }
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.child.close().await
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl FilterExecutor {
    /// Fetch one chunk from child.
    async fn fetch_one_chunk(&mut self) -> Result<Option<DataChunk>> {
        if let Some(data_chunk) = self.child.next().await? {
            let data_chunk = data_chunk.compact()?;
            let vis_array = self.expr.eval(&data_chunk)?;
            return if let Bool(vis) = vis_array.as_ref() {
                let vis = vis.try_into()?;
                let data_chunk = data_chunk.with_visibility(vis);
                Ok(Some(data_chunk))
            } else {
                Err(InternalError("Filter can only receive bool array".to_string()).into())
            };
        } else {
            Ok(None)
        }
    }
}

impl BoxedExecutorBuilder for FilterExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::Filter);
        ensure!(source.plan_node().get_children().len() == 1);
        let filter_node =
            FilterNode::decode(&(source.plan_node()).get_body().value[..]).map_err(ProstError)?;
        let expr_node = filter_node.get_search_condition();
        let expr = build_from_prost(expr_node)?;
        if let Some(child_plan) = source.plan_node.get_children().get(0) {
            let child = source.clone_for_plan(child_plan).build()?;
            debug!("Child schema: {:?}", child.schema());
            let chunk_builder =
                DataChunkBuilder::new(child.schema().data_types(), DEFAULT_CHUNK_BUFFER_SIZE);

            return Ok(Box::new(Self {
                expr,
                child,
                chunk_builder,
                last_input: None,
                identity: format!("FilterExecutor{:?}", source.task_id),
            }));
        }
        Err(InternalError("Filter must have one children".to_string()).into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::column::Column;
    use risingwave_common::array::{Array, DataChunk, PrimitiveArray};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::expr::build_from_prost;
    use risingwave_common::types::DataTypeKind;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::expr::expr_node::Type::InputRef;
    use risingwave_pb::expr::expr_node::{RexNode, Type};
    use risingwave_pb::expr::{ExprNode, FunctionCall, InputRefExpr};

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::task::TaskId;

    #[tokio::test]
    async fn test_filter_executor() {
        let col1 = create_column(&[Some(2), Some(2)]).unwrap();
        let col2 = create_column(&[Some(1), Some(2)]).unwrap();
        let data_chunk = DataChunk::builder().columns([col1, col2].to_vec()).build();
        let schema = Schema {
            fields: vec![
                Field::new_without_name(DataTypeKind::Int32),
                Field::new_without_name(DataTypeKind::Int32),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(data_chunk);
        let expr = make_expression(Type::Equal);
        let chunk_builder = DataChunkBuilder::new(
            mock_executor.schema().data_types(),
            DEFAULT_CHUNK_BUFFER_SIZE,
        );
        let mut filter_executor = FilterExecutor {
            expr: build_from_prost(&expr).unwrap(),
            child: Box::new(mock_executor),
            chunk_builder,
            last_input: None,
            identity: format!("FilterExecutor{:?}", TaskId::default()),
        };
        let fields = &filter_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataTypeKind::Int32);
        assert_eq!(fields[1].data_type, DataTypeKind::Int32);
        filter_executor.open().await.unwrap();
        let res = filter_executor.next().await.unwrap();
        if let Some(res) = res {
            let col1 = res.column_at(0).unwrap();
            let array = col1.array();
            let col1 = array.as_int32();
            assert_eq!(col1.len(), 1);
        } else {
            panic!("Filter executor returned no data!")
        }
        filter_executor.close().await.unwrap();
    }

    fn make_expression(kind: Type) -> ExprNode {
        let lhs = make_inputref(0);
        let rhs = make_inputref(1);
        let function_call = FunctionCall {
            children: vec![lhs, rhs],
        };
        let return_type = risingwave_pb::data::DataType {
            type_name: risingwave_pb::data::data_type::TypeName::Boolean as i32,
            precision: 0,
            scale: 0,
            is_nullable: false,
            interval_type: 0,
        };
        ExprNode {
            expr_type: kind as i32,
            return_type: Some(return_type),
            rex_node: Some(RexNode::FuncCall(function_call)),
        }
    }

    fn make_inputref(idx: i32) -> ExprNode {
        ExprNode {
            expr_type: InputRef as i32,
            return_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            rex_node: Some(RexNode::InputRef(InputRefExpr { column_idx: idx })),
        }
    }

    fn create_column(vec: &[Option<i32>]) -> Result<Column> {
        let array = PrimitiveArray::from_slice(vec).map(|x| Arc::new(x.into()))?;
        Ok(Column::new(array))
    }
}
