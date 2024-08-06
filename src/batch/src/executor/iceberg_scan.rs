// Copyright 2024 RisingWave Labs
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

use std::mem;

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use iceberg::arrow::ArrowReader;
use iceberg::scan::FileScanTask;
use iceberg::spec::TableMetadata;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::iceberg::IcebergConfig;

use crate::error::BatchError;
use crate::executor::{DataChunk, Executor};

pub struct IcebergScanExecutor {
    iceberg_config: IcebergConfig,
    #[allow(dead_code)]
    snapshot_id: Option<i64>,
    table_meta: TableMetadata,
    file_scan_tasks: Vec<FileScanTask>,
    batch_size: usize,
    schema: Schema,
    identity: String,
}

impl Executor for IcebergScanExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl IcebergScanExecutor {
    pub fn new(
        iceberg_config: IcebergConfig,
        snapshot_id: Option<i64>,
        table_meta: TableMetadata,
        file_scan_tasks: Vec<FileScanTask>,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            table_meta,
            file_scan_tasks,
            batch_size,
            schema,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let table = self
            .iceberg_config
            .load_table_v2_with_metadata(self.table_meta)
            .await?;
        let data_types = self.schema.data_types();

        let tasks_len = self.file_scan_tasks.len();
        let file_scan_tasks = mem::take(&mut self.file_scan_tasks);

        let file_scan_stream = {
            #[try_stream]
            async move {
                for file_scan_task in file_scan_tasks {
                    yield file_scan_task;
                }
            }
        };

        let reader = table
            .reader_builder()
            .with_batch_size(self.batch_size)
            .build();

        let record_batch_stream = reader
            .read(Box::pin(file_scan_stream))
            .map_err(BatchError::Iceberg)?;

        #[for_await]
        for record_batch in record_batch_stream {
            let record_batch = record_batch.map_err(BatchError::Iceberg)?;
            let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
            debug_assert_eq!(chunk.data_types(), data_types);
            yield chunk;
        }
    }
}
