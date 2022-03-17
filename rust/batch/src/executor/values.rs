use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, DataChunk, I32Array};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::expr::{build_from_prost, BoxedExpression};
use risingwave_pb::plan::plan_node::NodeBody;

use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// `ValuesExecutor` implements Values executor.
pub(super) struct ValuesExecutor {
    rows: Vec<Vec<BoxedExpression>>,
    schema: Schema,
    identity: String,
}

impl ValuesExecutor {
    #[cfg(test)]
    pub(crate) fn new(rows: Vec<Vec<BoxedExpression>>, schema: Schema, identity: String) -> Self {
        Self {
            rows,
            schema,
            identity,
        }
    }
}

#[async_trait::async_trait]
impl Executor for ValuesExecutor {
    async fn open(&mut self) -> Result<()> {
        info!("Values executor init");
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.rows.is_empty() {
            return Ok(None);
        }

        let cardinality = self.rows.len();
        ensure!(cardinality > 0);

        let mut array_builders = self
            .rows
            .first()
            .ok_or_else(|| RwError::from(InternalError("Can't values empty rows!".to_string())))?
            .iter() // for each column
            .map(|col| {
                col.return_type()
                    .create_array_builder(cardinality)
                    .map_err(|_| {
                        RwError::from(InternalError(
                            "Creat array builder failed when values".to_string(),
                        ))
                    })
            })
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

        if self.schema.fields.is_empty() {
            let cardinality = self.rows.len();
            self.rows.clear();
            return Ok(Some(DataChunk::new_dummy(cardinality)));
        }

        let one_row_array = I32Array::from_slice(&[Some(1)])?;
        // We need a one row chunk rather than an empty chunk because constant expression's eval
        // result is same size as input chunk cardinality.
        let one_row_chunk = DataChunk::builder()
            .columns(vec![Column::new(Arc::new(one_row_array.into()))])
            .build();

        for row in self.rows.drain(0..self.rows.len()) {
            for (mut expr, builder) in row.into_iter().zip_eq(&mut array_builders) {
                let out = expr.eval(&one_row_chunk)?;
                builder.append_array(&out)?;
            }
        }

        let columns = array_builders
            .into_iter()
            .map(|builder| builder.finish().map(|arr| Column::new(Arc::new(arr))))
            .collect::<Result<Vec<Column>>>()?;

        let chunk = DataChunk::builder().columns(columns).build();

        Ok(Some(chunk))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl BoxedExecutorBuilder for ValuesExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let value_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Values
        )?;

        let mut rows: Vec<Vec<BoxedExpression>> = Vec::with_capacity(value_node.get_tuples().len());
        for row in value_node.get_tuples() {
            let expr_row = row
                .get_cells()
                .iter()
                .map(build_from_prost)
                .collect::<Result<Vec<BoxedExpression>>>()?;
            rows.push(expr_row);
        }

        let fields = value_node
            .get_fields()
            .iter()
            .map(Field::from)
            .collect::<Vec<Field>>();

        Ok(Box::new(Self {
            rows,
            schema: Schema { fields },
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array;
    use risingwave_common::array::{I16Array, I32Array, I64Array};
    use risingwave_common::expr::LiteralExpression;
    use risingwave_common::types::{DataType, ScalarImpl};

    use super::*;

    #[tokio::test]
    async fn test_values_executor() {
        let exprs = vec![
            Box::new(LiteralExpression::new(
                DataType::Int16,
                Some(ScalarImpl::Int16(1)),
            )) as BoxedExpression,
            Box::new(LiteralExpression::new(
                DataType::Int32,
                Some(ScalarImpl::Int32(2)),
            )),
            Box::new(LiteralExpression::new(
                DataType::Int64,
                Some(ScalarImpl::Int64(3)),
            )),
        ];

        let fields = exprs
            .iter() // for each column
            .map(|col| Field::unnamed(col.return_type()))
            .collect::<Vec<Field>>();
        let mut values_executor = ValuesExecutor {
            rows: vec![exprs],
            schema: Schema { fields },
            identity: "ValuesExecutor".to_string(),
        };
        values_executor.open().await.unwrap();

        let fields = &values_executor.schema().fields;
        assert_eq!(fields[0].data_type, DataType::Int16);
        assert_eq!(fields[1].data_type, DataType::Int32);
        assert_eq!(fields[2].data_type, DataType::Int64);

        values_executor.open().await.unwrap();
        let result = values_executor.next().await.unwrap().unwrap();
        values_executor.close().await.unwrap();
        assert_eq!(
            *result.column_at(0).array(),
            array! {I16Array, [Some(1 as i16)]}.into()
        );
        assert_eq!(
            *result.column_at(1).array(),
            array! {I32Array, [Some(2)]}.into()
        );
        assert_eq!(
            *result.column_at(2).array(),
            array! {I64Array, [Some(3)]}.into()
        );
    }

    // Handle the possible case of ValuesNode([[]])
    #[tokio::test]
    async fn test_no_column_values_executor() {
        let mut values_executor = ValuesExecutor {
            rows: vec![vec![]], // One single row with no column.
            schema: Schema::default(),
            identity: "ValuesExecutor".to_string(),
        };
        values_executor.open().await.unwrap();

        let result = values_executor.next().await.unwrap().unwrap();
        assert_eq!(result.cardinality(), 1);
        assert_eq!(result.dimension(), 0);

        assert_eq!(values_executor.next().await.unwrap().is_none(), true);
    }
}
