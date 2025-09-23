// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Schema, SysCatalogReaderRef, TableId};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};

pub struct SysRowSeqScanExecutor {
    table_id: TableId,
    schema: Schema,
    column_indices: Vec<usize>,
    identity: String,

    sys_catalog_reader: SysCatalogReaderRef,
}

impl SysRowSeqScanExecutor {
    pub fn new(
        table_id: TableId,
        schema: Schema,
        column_indices: Vec<usize>,
        identity: String,
        sys_catalog_reader: SysCatalogReaderRef,
    ) -> Self {
        Self {
            table_id,
            schema,
            column_indices,
            identity,
            sys_catalog_reader,
        }
    }
}

pub struct SysRowSeqScanExecutorBuilder {}

impl BoxedExecutorBuilder for SysRowSeqScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Row sequential scan should not have input executor!"
        );
        let seq_scan_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::SysRowSeqScan
        )?;
        let sys_catalog_reader = source.context().catalog_reader();

        let table_id = seq_scan_node.get_table_id().into();

        let column_indices = seq_scan_node
            .column_descs
            .iter()
            .map(|d| d.column_id as usize)
            .collect_vec();
        let schema = Schema::new(
            seq_scan_node
                .column_descs
                .iter()
                .map(Into::into)
                .collect_vec(),
        );
        Ok(Box::new(SysRowSeqScanExecutor::new(
            table_id,
            schema,
            column_indices,
            source.plan_node().get_identity().clone(),
            sys_catalog_reader,
        )))
    }
}

impl Executor for SysRowSeqScanExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl SysRowSeqScanExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        #[for_await]
        for chunk in self.sys_catalog_reader.read_table(self.table_id) {
            let chunk = chunk.map_err(BatchError::SystemTable)?;
            yield chunk.project(&self.column_indices);
        }
    }
}
