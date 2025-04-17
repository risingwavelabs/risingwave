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
use futures_util::stream::StreamExt;
use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{
    Field, ICEBERG_FILE_PATH_COLUMN_NAME, ICEBERG_SEQUENCE_NUM_COLUMN_NAME, Schema,
};
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::source::iceberg::{
    IcebergFileScanTask, IcebergProperties, IcebergScanOpts, IcebergSplit, scan_task_to_chunk,
};
use risingwave_connector::source::{ConnectorProperties, SplitImpl, SplitMetaData};
use risingwave_expr::expr::LiteralExpression;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder, ExecutorBuilder};
use crate::ValuesExecutor;
use crate::error::BatchError;
use crate::executor::Executor;
use crate::monitor::BatchMetrics;

pub struct IcebergScanExecutor {
    iceberg_config: IcebergProperties,
    #[allow(dead_code)]
    snapshot_id: Option<i64>,
    file_scan_tasks: Option<IcebergFileScanTask>,
    chunk_size: usize,
    schema: Schema,
    identity: String,
    metrics: Option<BatchMetrics>,
    need_seq_num: bool,
    need_file_path_and_pos: bool,
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
        iceberg_config: IcebergProperties,
        snapshot_id: Option<i64>,
        file_scan_tasks: IcebergFileScanTask,
        chunk_size: usize,
        schema: Schema,
        identity: String,
        metrics: Option<BatchMetrics>,
        need_seq_num: bool,
        need_file_path_and_pos: bool,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            chunk_size,
            schema,
            file_scan_tasks: Some(file_scan_tasks),
            identity,
            metrics,
            need_seq_num,
            need_file_path_and_pos,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(mut self: Box<Self>) {
        let table = self.iceberg_config.load_table().await?;
        let data_types = self.schema.data_types();

        let data_file_scan_tasks = match Option::take(&mut self.file_scan_tasks) {
            Some(IcebergFileScanTask::Data(data_file_scan_tasks)) => data_file_scan_tasks,
            Some(IcebergFileScanTask::EqualityDelete(equality_delete_file_scan_tasks)) => {
                equality_delete_file_scan_tasks
            }
            Some(IcebergFileScanTask::PositionDelete(position_delete_file_scan_tasks)) => {
                position_delete_file_scan_tasks
            }
            Some(IcebergFileScanTask::CountStar(_)) => {
                bail!("iceberg scan executor does not support count star")
            }
            None => {
                bail!("file_scan_tasks must be Some")
            }
        };

        for data_file_scan_task in data_file_scan_tasks {
            #[for_await]
            for chunk in scan_task_to_chunk(
                table.clone(),
                data_file_scan_task,
                IcebergScanOpts {
                    chunk_size: self.chunk_size,
                    need_seq_num: self.need_seq_num,
                    need_file_path_and_pos: self.need_file_path_and_pos,
                },
                self.metrics.as_ref().map(|m| m.iceberg_scan_metrics()),
            ) {
                let chunk = chunk?;
                assert_eq!(chunk.data_types(), data_types);
                yield chunk;
            }
        }
    }
}

pub struct IcebergScanExecutorBuilder {}

impl BoxedExecutorBuilder for IcebergScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
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
        let config = ConnectorProperties::extract(options_with_secret.clone(), false)?;

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
        let metrics = source.context().batch_metrics().clone();

        if let ConnectorProperties::Iceberg(iceberg_properties) = config
            && let SplitImpl::Iceberg(split) = &split_list[0]
        {
            if let IcebergFileScanTask::CountStar(count) = split.task {
                return Ok(Box::new(ValuesExecutor::new(
                    vec![vec![Box::new(LiteralExpression::new(
                        DataType::Int64,
                        Some(ScalarImpl::Int64(count as i64)),
                    ))]],
                    schema,
                    source.plan_node().get_identity().clone(),
                    source.context().get_config().developer.chunk_size,
                )));
            }
            let iceberg_properties: IcebergProperties = *iceberg_properties;
            let split: IcebergSplit = split.clone();
            let need_seq_num = schema
                .fields()
                .iter()
                .any(|f| f.name == ICEBERG_SEQUENCE_NUM_COLUMN_NAME);
            let need_file_path_and_pos = schema
                .fields()
                .iter()
                .any(|f| f.name == ICEBERG_FILE_PATH_COLUMN_NAME)
                && matches!(split.task, IcebergFileScanTask::Data(_));

            Ok(Box::new(IcebergScanExecutor::new(
                iceberg_properties,
                Some(split.snapshot_id),
                split.task,
                source.context().get_config().developer.chunk_size,
                schema,
                source.plan_node().get_identity().clone(),
                metrics,
                need_seq_num,
                need_file_path_and_pos,
            )))
        } else {
            unreachable!()
        }
    }
}
