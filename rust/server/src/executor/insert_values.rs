use crate::array::{ArrayRef, BoxedArrayBuilder, DataChunk};
use crate::catalog::TableId;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorBuilder, ExecutorResult};
use crate::expr::{build_from_proto, BoxedExpression, Datum};
use crate::storage::StorageManagerRef;
use crate::types::{DataType, Int32Type};
use crate::util::ProtobufConvert;
use protobuf::Message;
use risingwave_proto::plan::{InsertValueNode, PlanNode_PlanNodeType};
use std::convert::TryFrom;
use std::sync::Arc;

pub(super) struct InsertValuesExecutor {
    table_id: TableId,
    storage_manager: StorageManagerRef,
    exprs: Vec<Vec<BoxedExpression>>,
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

        let capacity = self.exprs.len();
        ensure!(capacity > 0);

        let mut array_builders = self
            .exprs
            .first()
            .ok_or_else(|| RwError::from(InternalError("Can't insert empty values!".to_string())))?
            .iter()
            .map(|e| e.return_type_ref())
            .map(|data_type_ref| DataType::create_array_builder(data_type_ref, capacity))
            .collect::<Result<Vec<BoxedArrayBuilder>>>()?;

        let empty_chunk = DataChunk::default();

        for row in &mut self.exprs {
            row.iter_mut()
                .zip(&mut array_builders)
                .map(|(expr, builder)| {
                    expr.eval(&empty_chunk)
                        .map(|out| builder.append_expr_output(out))
                        .map(|_| 1)
                })
                .collect::<Result<Vec<usize>>>()?;
        }

        let arrays = array_builders
            .into_iter()
            .map(|b| b.finish())
            .collect::<Result<Vec<ArrayRef>>>()?;

        let chunk = DataChunk::builder()
            .cardinality(capacity)
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
                .cardinality(capacity)
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

        let table_id = TableId::from_pb(insert_value_node.get_table_ref_id().clone())
            .map_err(|e| InternalError(format!("Failed to parser table id, reaseon: {:?}", e)))?;

        let storage_manager = source.task_context().storage_manager_ref();

        let mut exprs: Vec<Vec<BoxedExpression>> =
            Vec::with_capacity(insert_value_node.get_insert_tuples().len());
        for row in insert_value_node.get_insert_tuples() {
            let expr_row = row
                .get_cells()
                .iter()
                .map(|c| build_from_proto(c))
                .collect::<Result<Vec<BoxedExpression>>>()?;
            exprs.push(expr_row);
        }

        Ok(Self {
            table_id,
            storage_manager,
            exprs,
            executed: false,
        })
    }
}
