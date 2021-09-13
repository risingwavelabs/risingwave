use crate::array2::column::Column;
use crate::array2::{
    Array, ArrayBuilder, ArrayBuilderImpl, DataChunk, I32Array, PrimitiveArrayBuilder,
};
use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{build_from_proto, BoxedExpression};
use crate::storage::StorageManagerRef;

use crate::types::{DataType, Int32Type};
use pb_convert::FromProtobuf;
use protobuf::Message;
use risingwave_proto::plan::{InsertValueNode, PlanNode_PlanNodeType};
use std::convert::TryFrom;
use std::sync::Arc;

pub(super) struct InsertValuesExecutor {
    table_id: TableId,
    storage_manager: StorageManagerRef,

    // The rows to be inserted. Each row is composed of multiple values,
    // each value is represented by an expression.
    rows: Vec<Vec<BoxedExpression>>,
    executed: bool,
}

impl Executor for InsertValuesExecutor {
    fn init(&mut self) -> Result<()> {
        info!("Insert values executor");
        Ok(())
    }

    fn execute(&mut self) -> Result<ExecutorResult> {
        if self.executed {
            return Ok(Done);
        }

        let cardinality = self.rows.len();
        ensure!(cardinality > 0);

        let mut array_builders = self
            .rows
            .first()
            .ok_or_else(|| RwError::from(InternalError("Can't insert empty values!".to_string())))?
            .iter() // for each column
            .map(|col| {
                DataType::create_array_builder(col.return_type_ref(), cardinality).map_err(|_| {
                    RwError::from(InternalError(
                        "Creat array builder failed when insert values".to_string(),
                    ))
                })
            })
            .collect::<Result<Vec<ArrayBuilderImpl>>>()?;

        let one_row_array = I32Array::from_slice(&[Some(1)])?;
        // We need a one row chunk rather than an empty chunk because constant expression's eval result
        // is same size as input chunk cardinality.
        let one_row_chunk = DataChunk::builder()
            .cardinality(1)
            .columns(vec![Column::new(
                Arc::new(one_row_array.into()),
                Arc::new(Int32Type::new(false)),
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

        let chunk = DataChunk::builder()
            .cardinality(cardinality)
            .columns(columns)
            .build();

        let rows_inserted = self
            .storage_manager
            .get_table(&self.table_id)?
            .append(chunk)?;

        // create ret value
        {
            let mut array_builder = PrimitiveArrayBuilder::<i32>::new(1)?;
            array_builder.append(Some(rows_inserted as i32))?;

            let array = array_builder.finish()?;
            let ret_chunk = DataChunk::builder()
                .cardinality(array.len())
                .columns(vec![Column::new(
                    Arc::new(array.into()),
                    Arc::new(Int32Type::new(false)),
                )])
                .build();

            self.executed = true;
            Ok(ExecutorResult::Batch(Arc::new(ret_chunk)))
        }
    }

    fn clean(&mut self) -> Result<()> {
        info!("Cleaning insert values executor.");
        Ok(())
    }
}

impl<'a> TryFrom<&'a ExecutorBuilder<'a>> for InsertValuesExecutor {
    type Error = RwError;

    fn try_from(source: &'a ExecutorBuilder<'a>) -> Result<Self> {
        ensure!(source.plan_node().get_node_type() == PlanNode_PlanNodeType::INSERT_VALUE);
        let insert_value_node =
            InsertValueNode::parse_from_bytes(source.plan_node().get_body().get_value())
                .map_err(ProtobufError)?;

        let table_id = TableId::from_protobuf(insert_value_node.get_table_ref_id())
            .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))?;

        let storage_manager = source.global_task_env().storage_manager_ref();

        let mut rows: Vec<Vec<BoxedExpression>> =
            Vec::with_capacity(insert_value_node.get_insert_tuples().len());
        for row in insert_value_node.get_insert_tuples() {
            let expr_row = row
                .get_cells()
                .iter()
                .map(|c| build_from_proto(c))
                .collect::<Result<Vec<BoxedExpression>>>()?;
            rows.push(expr_row);
        }

        Ok(Self {
            table_id,
            storage_manager,
            rows,
            executed: false,
        })
    }
}
