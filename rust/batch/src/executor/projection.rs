use prost::Message;
use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::ProstError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::plan::plan_node::PlanNodeType;
use risingwave_pb::plan::ProjectNode;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{Executor, ExecutorBuilder};

pub(super) struct ProjectionExecutor {
    expr: Vec<BoxedExpression>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
}

#[async_trait::async_trait]
impl Executor for ProjectionExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        let child_output = self.child.next().await?;
        match child_output {
            Some(child_chunk) => {
                let arrays: Vec<Column> = self
                    .expr
                    .iter_mut()
                    .map(|expr| expr.eval(&child_chunk).map(Column::new))
                    .collect::<Result<Vec<_>>>()?;
                let ret = DataChunk::builder().columns(arrays).build();
                Ok(Some(ret))
            }
            None => Ok(None),
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.child.close().await?;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl BoxedExecutorBuilder for ProjectionExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNodeType::Project);
        ensure!(source.plan_node().get_children().len() == 1);

        let project_node =
            ProjectNode::decode(&(source.plan_node()).get_body().value[..]).map_err(ProstError)?;
        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child_node = source.clone_for_plan(proto_child).build()?;

        let project_exprs = project_node
            .get_select_list()
            .iter()
            .map(build_from_prost)
            .collect::<Result<Vec<BoxedExpression>>>()?;

        let fields = project_exprs
            .iter()
            .map(|expr| Field::new_without_name(expr.return_type().to_data_type()))
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            expr: project_exprs,
            child: child_node,
            schema: Schema { fields },
            identity: format!("ProjectionExecutor{:?}", source.task_id),
        }))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, I32Array};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::column_nonnull;
    use risingwave_common::expr::InputRefExpression;
    use risingwave_common::types::{DataTypeKind, Int32Type};

    use super::*;
    use crate::executor::test_utils::MockExecutor;
    use crate::task::TaskId;
    use crate::*;

    #[tokio::test]
    async fn test_project_executor() -> Result<()> {
        let col1 = column_nonnull! {I32Array, [1, 2, 33333, 4, 5]};
        let col2 = column_nonnull! {I32Array, [7, 8, 66666, 4, 3]};
        let chunk = DataChunk::builder().columns(vec![col1, col2]).build();

        let expr1 = InputRefExpression::new(DataTypeKind::Int32, 0);
        let expr_vec = vec![Box::new(expr1) as BoxedExpression];

        let schema = Schema {
            fields: vec![
                Field::new(Int32Type::create(false), String::from("")),
                Field::new(Int32Type::create(false), String::from("")),
            ],
        };
        let mut mock_executor = MockExecutor::new(schema);
        mock_executor.add(chunk);

        let fields = expr_vec
            .iter()
            .map(|expr| Field::new_without_name(expr.return_type().to_data_type()))
            .collect::<Vec<Field>>();

        let mut proj_executor = ProjectionExecutor {
            expr: expr_vec,
            child: Box::new(mock_executor),
            schema: Schema { fields },
            identity: format!("ProjectionExecutor{:?}", TaskId::default()),
        };
        proj_executor.open().await.unwrap();

        let fields = &proj_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int32);

        let result_chunk = proj_executor.next().await?.unwrap();
        proj_executor.close().await.unwrap();
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

        proj_executor.close().await.unwrap();
        Ok(())
    }
}
