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
use hashbrown::HashMap;
use iceberg::scan::FileScanTask;
use iceberg::spec::TableMetadata;
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarRefImpl};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_connector::sink::iceberg::IcebergConfig;
use risingwave_connector::source::iceberg::{IcebergProperties, IcebergSplit};
use risingwave_connector::source::{ConnectorProperties, SplitImpl, SplitMetaData};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::error::BatchError;
use crate::executor::{DataChunk, Executor};
use crate::task::BatchTaskContext;

pub struct IcebergScanExecutor {
    iceberg_config: IcebergConfig,
    #[allow(dead_code)]
    snapshot_id: Option<i64>,
    table_meta: TableMetadata,
    file_scan_tasks: Vec<FileScanTask>,
    delete_file_scan_tasks: Vec<FileScanTask>,
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
        delete_file_scan_tasks: Vec<FileScanTask>,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            table_meta,
            file_scan_tasks,
            delete_file_scan_tasks,
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

        let delete_file_scan_tasks = mem::take(&mut self.delete_file_scan_tasks);
        let delete_file_scan_stream = {
            #[try_stream]
            async move {
                for delete_file_scan_task in delete_file_scan_tasks {
                    yield delete_file_scan_task;
                }
            }
        };

        let reader = table
            .reader_builder()
            .with_batch_size(self.batch_size)
            .build();

        let mut delete_record_batch_stream = reader
            .read(Box::pin(delete_file_scan_stream))
            .map_err(BatchError::Iceberg)?;
        let mut map = HashMap::new();
        for record_batch in delete_record_batch_stream.next().await {
            let record_batch = record_batch.map_err(BatchError::Iceberg)?;
            let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
            for row in chunk.rows(){
                if let Some(ScalarRefImpl::Int64(i)) = row.datum_at(0){
                    map.insert(i,1);   
                }else{
                    unreachable!();
                }
            }
        }
        let mut data_chunk_builder =
            DataChunkBuilder::new(data_types.clone(), self.batch_size);
        #[for_await]
        for record_batch in record_batch_stream {
            // let vis_array = self.expr.eval(&data_chunk).await?;
            let record_batch = record_batch.map_err(BatchError::Iceberg)?;
            let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
            for row in chunk.rows(){
                if let Some(ScalarRefImpl::Int64(i)) = row.datum_at(0){
                    if !map.contains_key(&i) {
                        data_chunk_builder.append_one_row_no_finish(row);
                    } 
                }else{
                    unreachable!();
                }
            }
            
            debug_assert_eq!(chunk.data_types(), data_types);
            yield data_chunk_builder.build_data_chunk();
        }
    }
}

pub struct IcebergScanExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for IcebergScanExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Iceberg source should not have input executor!"
        );
        let source_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::IcebergScan
        )?;

        // prepare connector source
        let options_with_secret = WithOptionsSecResolved::new(
            source_node.with_properties.clone(),
            source_node.secret_refs.clone(),
        );
        let config = ConnectorProperties::extract(options_with_secret.clone(), false)
            .map_err(BatchError::connector)?;

        let split_list = source_node
            .split
            .iter()
            .map(|split| SplitImpl::restore_from_bytes(split).unwrap())
            .collect_vec();
        assert_eq!(split_list.len(), 1);

        let fields = source_node
            .columns
            .iter()
            .map(|prost| {
                let column_desc = prost.column_desc.as_ref().unwrap();
                let data_type = DataType::from(column_desc.column_type.as_ref().unwrap());
                let name = column_desc.name.clone();
                Field::with_name(data_type, name)
            })
            .collect();
        let schema = Schema::new(fields);

        if let ConnectorProperties::Iceberg(iceberg_properties) = config
            && let SplitImpl::Iceberg(split) = &split_list[0]
        {
            let iceberg_properties: IcebergProperties = *iceberg_properties;
            let split: IcebergSplit = split.clone();
            Ok(Box::new(IcebergScanExecutor::new(
                iceberg_properties.to_iceberg_config(),
                Some(split.snapshot_id),
                split.table_meta.deserialize(),
                split.files.into_iter().map(|x| x.deserialize()).collect(),
                split.delete_files.into_iter().map(|x| x.deserialize()).collect(),
                source.context.get_config().developer.chunk_size,
                schema,
                source.plan_node().get_identity().clone(),
            )))
        } else {
            unreachable!()
        }
    }
}
