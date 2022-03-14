use std::sync::Arc;

use futures::future::try_join_all;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilder, DataChunk, Op, PrimitiveArrayBuilder, StreamChunk};
use risingwave_common::catalog::{Field, Schema, TableId};
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_source::SourceManagerRef;

use super::BoxedExecutor;
use crate::executor::{BoxedExecutorBuilder, Executor, ExecutorBuilder};

/// [`DeleteExecutor`] implements table deletion with values from its child executor.
pub struct DeleteExecutor {
    /// Target table id.
    table_id: TableId,
    source_manager: SourceManagerRef,
    worker_id: u32,

    child: BoxedExecutor,
    executed: bool,
    schema: Schema,
    identity: String,
}

impl DeleteExecutor {
    pub fn new(
        table_id: TableId,
        source_manager: SourceManagerRef,
        child: BoxedExecutor,
        worker_id: u32,
    ) -> Self {
        Self {
            table_id,
            source_manager,
            worker_id,
            child,
            executed: false,
            // TODO: support `RETURNING`
            schema: Schema {
                fields: vec![Field::unnamed(DataType::Int64)],
            },
            identity: "DeleteExecutor".to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Executor for DeleteExecutor {
    async fn open(&mut self) -> Result<()> {
        self.child.open().await?;
        info!("Delete executor");
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.executed {
            return Ok(None);
        }

        let source_desc = self.source_manager.get_source(&self.table_id)?;
        let source = source_desc.source.as_table_v2();

        let mut notifiers = Vec::new();

        while let Some(child_chunk) = self.child.next().await? {
            let len = child_chunk.cardinality();
            assert!(child_chunk.visibility().is_none());

            let chunk = StreamChunk::from_parts(vec![Op::Delete; len], child_chunk);

            let notifier = source.write_chunk(chunk)?;
            notifiers.push(notifier);
        }

        // Wait for all chunks to be taken / written.
        let rows_inserted = try_join_all(notifiers)
            .await
            .map_err(|_| {
                RwError::from(ErrorCode::InternalError(
                    "failed to wait chunks to be written".to_owned(),
                ))
            })?
            .into_iter()
            .sum::<usize>();

        // create ret value
        {
            let mut array_builder = PrimitiveArrayBuilder::<i64>::new(1)?;
            array_builder.append(Some(rows_inserted as i64))?;

            let array = array_builder.finish()?;
            let ret_chunk = DataChunk::builder()
                .columns(vec![Column::new(Arc::new(array.into()))])
                .build();

            self.executed = true;
            Ok(Some(ret_chunk))
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.child.close().await?;
        info!("Cleaning delete executor.");
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

impl BoxedExecutorBuilder for DeleteExecutor {
    fn new_boxed_executor(source: &ExecutorBuilder) -> Result<BoxedExecutor> {
        let delete_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Delete
        )?;

        let table_id = TableId::from(&delete_node.table_source_ref_id);

        let proto_child = source.plan_node.get_children().get(0).ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(String::from(
                "Child interpreting error",
            )))
        })?;
        let child = source.clone_for_plan(proto_child).build()?;

        Ok(Box::new(Self::new(
            table_id,
            source.global_batch_env().source_manager_ref(),
            child,
            source.global_batch_env().worker_id(),
        )))
    }
}
