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

use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{ColumnDesc, Schema, SysCatalogReaderRef};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::executor::{
    BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder,
};
use crate::task::BatchTaskContext;

pub struct SysRowSeqScanExecutor {
    table_name: String,
    schema: Schema,
    identity: String,

    sys_catalog_reader: SysCatalogReaderRef,
}

impl SysRowSeqScanExecutor {
    pub fn new(
        table_name: String,
        schema: Schema,
        identity: String,
        sys_catalog_reader: SysCatalogReaderRef,
    ) -> Self {
        Self {
            table_name,
            schema,
            identity,
            sys_catalog_reader,
        }
    }
}

pub struct SysRowSeqScanExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for SysRowSeqScanExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
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
        // TODO: get system table data entry from BatchTaskContext.
        let sys_catalog_reader = source.context.sys_catalog_reader_ref();

        let table_name = seq_scan_node.table_name.clone();
        let column_descs = seq_scan_node
            .column_descs
            .iter()
            .map(|column_desc| ColumnDesc::from(column_desc.clone()))
            .collect_vec();

        let schema = Schema::new(column_descs.iter().map(Into::into).collect_vec());
        Ok(Box::new(SysRowSeqScanExecutor::new(
            table_name,
            schema,
            source.plan_node().get_identity().clone(),
            sys_catalog_reader.unwrap(),
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
        self.do_executor()
    }
}

impl SysRowSeqScanExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_executor(self: Box<Self>) {
        let rows = self.sys_catalog_reader.read_table(&self.table_name).await?;
        let chunk =
            DataChunk::from_rows(&rows, &self.schema.data_types()).map_err(RwError::from)?;
        yield chunk
    }
}
