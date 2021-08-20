use crate::array::{ArrayRef, BoxedArrayBuilder, DataChunk, PrimitiveArray};
use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{build_from_proto, BoxedExpression, Datum};
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
            .map(|col| DataType::create_array_builder(col.return_type_ref(), cardinality))
            .collect::<Result<Vec<BoxedArrayBuilder>>>()?;

        let one_row_array = PrimitiveArray::<Int32Type>::from_slice(vec![1])?;
        // We need a one row chunk rather than an empty chunk because constant expression's eval result
        // is same size as input chunk cardinality.
        let one_row_chunk = DataChunk::builder()
            .cardinality(1)
            .arrays(vec![one_row_array])
            .build();

        for row in &mut self.rows {
            row.iter_mut()
                .zip(&mut array_builders)
                .map(|(expr, builder)| {
                    expr.eval(&one_row_chunk)
                        .and_then(|out| builder.append_array(&*out))
                        .map(|_| 1)
                })
                .collect::<Result<Vec<usize>>>()?;
        }

        let arrays = array_builders
            .into_iter()
            .map(|b| b.finish())
            .collect::<Result<Vec<ArrayRef>>>()?;

        let chunk = DataChunk::builder()
            .cardinality(cardinality)
            .arrays(arrays)
            .build();

        let rows_inserted = self
            .storage_manager
            .get_table(&self.table_id)?
            .append(chunk)?;

        // create ret value
        {
            let data_type = Arc::new(Int32Type::new(false));
            let mut array_builder = DataType::create_array_builder(data_type, 1)?;
            array_builder.append(&Datum::Int32(rows_inserted as i32))?;

            let array = array_builder.finish()?;
            let ret_chunk = DataChunk::builder()
                .cardinality(array.len())
                .arrays(vec![array])
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
