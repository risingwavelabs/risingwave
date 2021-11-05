use std::sync::Arc;

use protobuf::Message;

use risingwave_proto::plan::{PlanNode_PlanNodeType, ValuesNode};

use crate::executor::ExecutorResult::Done;
use crate::executor::{
    BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder, ExecutorResult,
};
use risingwave_common::array::column::Column;
use risingwave_common::array::I32Array;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::{InternalError, ProtobufError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::expr::{build_from_proto, BoxedExpression};
use risingwave_common::types::{DataType, Int32Type};

/// `ValuesExecutor` implements Values executor.
pub(super) struct ValuesExecutor {
    rows: Vec<Vec<BoxedExpression>>,
    executed: bool,
    schema: Schema,
}

#[async_trait::async_trait]
impl Executor for ValuesExecutor {
    fn init(&mut self) -> Result<()> {
        info!("Values executor");
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        if self.executed {
            return Ok(Done);
        }

        let cardinality = self.rows.len();
        ensure!(cardinality > 0);

        let mut array_builders = self
            .rows
            .first()
            .ok_or_else(|| RwError::from(InternalError("Can't values empty rows!".to_string())))?
            .iter() // for each column
            .map(|col| {
                DataType::create_array_builder(col.return_type_ref(), cardinality).map_err(|_| {
                    RwError::from(InternalError(
                        "Creat array builder failed when values".to_string(),
                    ))
                })
            })
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

        let one_row_array = I32Array::from_slice(&[Some(1)])?;
        // We need a one row chunk rather than an empty chunk because constant expression's eval result
        // is same size as input chunk cardinality.
        let one_row_chunk = DataChunk::builder()
            .columns(vec![Column::new(
                Arc::new(one_row_array.into()),
                Int32Type::create(false),
            )])
            .build();

        for row in &mut self.rows {
            row.iter_mut()
                .zip(&mut array_builders)
                .map(|(expr, builder)| {
                    expr.eval(&one_row_chunk)
                        .and_then(|out| builder.append_array(&out))
                        .map(|_| 1)
                })
                .collect::<Result<Vec<usize>>>()?;
        }

        let columns = array_builders
            .into_iter()
            .zip(self.rows[0].iter())
            .map(|(builder, expr)| {
                builder
                    .finish()
                    .map(|arr| Column::new(Arc::new(arr), expr.return_type_ref()))
            })
            .collect::<Result<Vec<Column>>>()?;

        let chunk = DataChunk::builder().columns(columns).build();

        self.executed = true;
        Ok(ExecutorResult::Batch(chunk))
    }

    fn clean(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for ValuesExecutor {
    async fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::VALUE);
        let value_node = ValuesNode::parse_from_bytes(source.plan_node().get_body().get_value())
            .map_err(ProtobufError)?;

        let mut rows: Vec<Vec<BoxedExpression>> = Vec::with_capacity(value_node.get_tuples().len());
        for row in value_node.get_tuples() {
            let expr_row = row
                .get_cells()
                .iter()
                .map(build_from_proto)
                .collect::<Result<Vec<BoxedExpression>>>()?;
            rows.push(expr_row);
        }

        let fields = rows
            .first()
            .ok_or_else(|| RwError::from(InternalError("Can't values empty rows!".to_string())))?
            .iter() // for each column
            .map(|col| Field {
                data_type: col.return_type_ref(),
            })
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            rows,
            executed: false,
            schema: Schema { fields },
        }))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::Array;
    use risingwave_common::expr::LiteralExpression;
    use risingwave_common::types::{DataTypeKind, Int16Type, Int32Type, Int64Type, ScalarImpl};

    use super::*;

    #[tokio::test]
    async fn test_values_executor() -> Result<()> {
        let exprs = vec![vec![
            Box::new(LiteralExpression::new(
                Int16Type::create(false),
                Some(ScalarImpl::Int16(1)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                Int32Type::create(false),
                Some(ScalarImpl::Int32(2)),
            )),
            Box::new(LiteralExpression::new(
                Int64Type::create(false),
                Some(ScalarImpl::Int64(3)),
            )),
        ]];

        let fields = exprs
            .first()
            .ok_or_else(|| RwError::from(InternalError("Can't values empty rows!".to_string())))?
            .iter() // for each column
            .map(|col| Field {
                data_type: col.return_type_ref(),
            })
            .collect::<Vec<Field>>();
        let mut values_executor = ValuesExecutor {
            rows: exprs,
            executed: false,
            schema: Schema { fields },
        };
        assert!(values_executor.init().is_ok());

        let fields = &values_executor.schema().fields;
        assert_eq!(fields[0].data_type.data_type_kind(), DataTypeKind::Int16);
        assert_eq!(fields[1].data_type.data_type_kind(), DataTypeKind::Int32);
        assert_eq!(fields[2].data_type.data_type_kind(), DataTypeKind::Int64);

        let result = values_executor.execute().await?.batch_or()?;
        assert!(values_executor.clean().is_ok());
        assert_eq!(
            result
                .column_at(0)?
                .array()
                .as_int16()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1)]
        );
        assert_eq!(
            result
                .column_at(1)?
                .array()
                .as_int32()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2)]
        );
        assert_eq!(
            result
                .column_at(2)?
                .array()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(3)]
        );
        Ok(())
    }
}
