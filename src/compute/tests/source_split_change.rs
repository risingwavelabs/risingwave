// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use futures::stream::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_batch::executor::monitor::BatchMetrics;
use risingwave_batch::executor::{
    BoxedDataChunkStream, BoxedExecutor, DeleteExecutor, Executor as BatchExecutor, InsertExecutor,
    RowSeqScanExecutor,
};
use risingwave_common::array::{Array, DataChunk, F64Array, I64Array, Row};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
use risingwave_common::column_nonnull;
use risingwave_common::error::{Result, RwError};
use risingwave_common::test_prelude::DataChunkTestExt;
use risingwave_common::types::{DataType, IntoOrdered};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::data::data_type::TypeName;
use risingwave_pb::plan_common::ColumnDesc as ProstColumnDesc;
use risingwave_source::{MemSourceManager, SourceManager};
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::monitor::StateStoreMetrics;
use risingwave_storage::table::cell_based_table::CellBasedTable;
use risingwave_storage::table::state_table::StateTable;
use risingwave_storage::Keyspace;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::executor::{
    Barrier, Executor as StreamExecutor, MaterializeExecutor, Message, PkIndices, SourceExecutor,
};
use tokio::sync::mpsc::unbounded_channel;

// the test checks whether change split mutation works
// the test relies on external kafka to complete
#[tokio::test]
async fn test_split_change_mutation() -> Result<()> {
    use risingwave_pb::data::DataType;

    let memory_state_store = MemoryStateStore::new();
    let source_manager = Arc::new(MemSourceManager::default());
    let source_table_id = TableId::default();
    let table_columns: Vec<ColumnDesc> = vec![
        // row id
        ProstColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
            column_id: 0,
            ..Default::default()
        }
        .into(),
        // data
        ProstColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Double as i32,
                ..Default::default()
            }),
            column_id: 1,
            ..Default::default()
        }
        .into(),
    ];
    source_manager.create_table_source(&source_table_id, table_columns)?;

    // Ensure the source exists
    let source_desc = source_manager.get_source(&source_table_id)?;
    let get_schema = |column_ids: &[ColumnId]| {
        let mut fields = Vec::with_capacity(column_ids.len());
        for &column_id in column_ids {
            let column_desc = source_desc
                .columns
                .iter()
                .find(|c| c.column_id == column_id)
                .unwrap();
            fields.push(Field::unnamed(column_desc.data_type.clone()));
        }
        Schema::new(fields)
    };

     // Create a `SourceExecutor` to read the changes
     let all_column_ids = vec![ColumnId::from(0), ColumnId::from(1)];
     let all_schema = get_schema(&all_column_ids);
     let (barrier_tx, barrier_rx) = unbounded_channel();
     let keyspace = Keyspace::executor_root(MemoryStateStore::new(), 0x2333);
     let stream_source = SourceExecutor::new(
         0x3f3f3f,
         source_table_id,
         source_desc.clone(),
         keyspace,
         all_column_ids.clone(),
         all_schema.clone(),
         PkIndices::from([0]),
         barrier_rx,
         1,
         1,
         "SourceExecutor".to_string(),
         Arc::new(StreamingMetrics::unused()),
         vec![],
         u64::MAX,
     )?;
    Ok(())
}
