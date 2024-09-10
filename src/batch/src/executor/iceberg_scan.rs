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

use core::ops::BitOr;
use std::collections::HashMap;
use std::mem;

use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::TableMetadata;
use itertools::Itertools;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::iter_util::ZipEqFast;
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
    data_file_scan_tasks: Vec<FileScanTask>,
    eq_delete_file_scan_tasks: Vec<FileScanTask>,
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
        data_file_scan_tasks: Vec<FileScanTask>,
        eq_delete_file_scan_tasks: Vec<FileScanTask>,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            table_meta,
            data_file_scan_tasks,
            eq_delete_file_scan_tasks,
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
        let chunk_schema_name_to_id = self
            .schema
            .names()
            .iter()
            .enumerate()
            .map(|(k, v)| (v.clone(), k))
            .collect::<HashMap<_, _>>();

        // The value to remove from the column and its seq_num.
        let mut eq_delete_file_scan_tasks_map: HashMap<
            String,
            HashMap<Option<risingwave_common::types::ScalarImpl>, i64>,
        > = HashMap::default();
        let eq_delete_file_scan_tasks = mem::take(&mut self.eq_delete_file_scan_tasks);

        for mut eq_delete_file_scan_task in eq_delete_file_scan_tasks {
            eq_delete_file_scan_task.project_field_ids =
                eq_delete_file_scan_task.equality_ids.clone();
            let mut sequence_number = eq_delete_file_scan_task.sequence_number;
            let reader = table
                .reader_builder()
                .with_batch_size(self.batch_size)
                .build();
            let delete_file_scan_stream = tokio_stream::once(Ok(eq_delete_file_scan_task));

            let mut delete_record_batch_stream = reader
                .read(Box::pin(delete_file_scan_stream))
                .map_err(BatchError::Iceberg)?;

            while let Some(record_batch) = delete_record_batch_stream.next().await {
                let record_batch = record_batch.map_err(BatchError::Iceberg)?;
                let delete_column_names = record_batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name())
                    .cloned()
                    .collect_vec();

                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
                for (array, columns_name) in chunk.columns().iter().zip_eq_fast(delete_column_names)
                {
                    let each_column_seq_num_map = eq_delete_file_scan_tasks_map
                        .entry(columns_name)
                        .or_default();
                    for datum in array.get_all_values() {
                        let entry = each_column_seq_num_map
                            .entry(datum)
                            .or_insert(sequence_number);
                        *entry = *entry.max(&mut sequence_number);
                    }
                }
            }
        }

        let data_file_scan_tasks = mem::take(&mut self.data_file_scan_tasks);

        for data_file_scan_task in data_file_scan_tasks {
            let data_sequence_number = data_file_scan_task.sequence_number;
            let reader = table
                .reader_builder()
                .with_batch_size(self.batch_size)
                .build();
            let file_scan_stream = tokio_stream::once(Ok(data_file_scan_task));

            let mut record_batch_stream = reader
                .read(Box::pin(file_scan_stream))
                .map_err(BatchError::Iceberg)?;

            while let Some(record_batch) = record_batch_stream.next().await {
                let record_batch = record_batch.map_err(BatchError::Iceberg)?;
                let column_names = record_batch
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name())
                    .cloned()
                    .collect_vec();
                let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
                let visibilitys: Vec<_> = chunk
                    .columns()
                    .iter()
                    .zip_eq_fast(column_names.clone())
                    .filter_map(|(array, column_map)| {
                        if let Some(each_column_seq_num_map) =
                            eq_delete_file_scan_tasks_map.get(&column_map)
                        {
                            let visibility =
                                Bitmap::from_iter(array.get_all_values().iter().map(|datum| {
                                    if let Some(delete_sequence_number) =
                                        each_column_seq_num_map.get(datum)
                                        && delete_sequence_number > &data_sequence_number
                                    {
                                        false
                                    } else {
                                        true
                                    }
                                }));
                            Some(visibility)
                        } else {
                            None
                        }
                    })
                    .collect();
                let (data, chunk_visibilitys) = chunk.into_parts_v2();
                let visibility = if visibilitys.is_empty() {
                    chunk_visibilitys
                } else {
                    // Calculate the result of the or operation for different columns of the bitmap
                    visibilitys
                        .iter()
                        .skip(1)
                        .fold(visibilitys[0].clone(), |acc, bitmap| acc.bitor(bitmap))
                };
                let data = data
                    .iter()
                    .zip_eq_fast(column_names)
                    .filter_map(|(array, columns)| {
                        chunk_schema_name_to_id
                            .get(&columns)
                            .map(|&id| (id, array.clone()))
                    })
                    .sorted_by_key(|a| a.0)
                    .map(|(_k, v)| v)
                    .collect_vec();
                let chunk = DataChunk::new(data, visibility);
                debug_assert_eq!(chunk.data_types(), data_types);
                yield chunk;
            }
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
                split
                    .eq_delete_files
                    .into_iter()
                    .map(|x| x.deserialize())
                    .collect(),
                source.context.get_config().developer.chunk_size,
                schema,
                source.plan_node().get_identity().clone(),
            )))
        } else {
            unreachable!()
        }
    }
}
