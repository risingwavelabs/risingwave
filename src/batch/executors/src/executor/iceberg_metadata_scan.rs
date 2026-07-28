// Copyright 2026 RisingWave Labs
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
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_connector::sink::iceberg::{
    IcebergMetadataFilter, IcebergMetadataTableType, scan_iceberg_metadata,
};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergTimeTravelInfo;
use risingwave_pb::batch_plan::iceberg_metadata_scan_node::{MetadataType, TimeTravel};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, anyhow};
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};

pub struct IcebergMetadataScanExecutor {
    schema: Schema,
    properties: risingwave_connector::source::iceberg::IcebergProperties,
    metadata_type: IcebergMetadataTableType,
    time_travel_info: Option<IcebergTimeTravelInfo>,
    filter: IcebergMetadataFilter,
    output_indices: Vec<usize>,
    limit: Option<u64>,
    identity: String,
    chunk_size: usize,
}

impl Executor for IcebergMetadataScanExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl IcebergMetadataScanExecutor {
    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let table = self.properties.load_table().await?;
        #[for_await]
        for chunk in scan_iceberg_metadata(
            table,
            self.metadata_type,
            self.time_travel_info,
            self.filter,
            self.limit,
            self.chunk_size,
        ) {
            yield chunk?.project(&self.output_indices);
        }
    }
}

pub struct IcebergMetadataScanExecutorBuilder;

impl BoxedExecutorBuilder for IcebergMetadataScanExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> crate::error::Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "Iceberg metadata scan should not have input executors"
        );
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::IcebergMetadataScan
        )?;

        let metadata_type = match MetadataType::try_from(node.metadata_type)
            .map_err(|error| anyhow!("invalid Iceberg metadata type: {error}"))?
        {
            MetadataType::Snapshots => IcebergMetadataTableType::Snapshots,
            MetadataType::Manifests => IcebergMetadataTableType::Manifests,
            MetadataType::Files => IcebergMetadataTableType::Files,
            MetadataType::Unspecified => {
                return Err(anyhow!("Iceberg metadata type is unspecified").into());
            }
        };
        let time_travel_info = node
            .time_travel
            .as_ref()
            .map(|time_travel| match time_travel {
                TimeTravel::SnapshotId(snapshot_id) => IcebergTimeTravelInfo::Version(*snapshot_id),
                TimeTravel::TimestampMs(timestamp_ms) => {
                    IcebergTimeTravelInfo::TimestampMs(*timestamp_ms)
                }
            });
        let full_column_count = metadata_type.schema().len();
        let output_indices = node
            .columns
            .iter()
            .map(|column| {
                let index = usize::try_from(column.column_id).map_err(|_| {
                    anyhow!(
                        "invalid Iceberg metadata column id {}",
                        column.column_id
                    )
                })?;
                if index >= full_column_count {
                    return Err(anyhow!(
                        "Iceberg metadata column id {index} exceeds schema width {full_column_count}"
                    ));
                }
                Ok(index)
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let config = ConnectorProperties::extract(
            WithOptionsSecResolved::new(node.with_properties.clone(), node.secret_refs.clone()),
            false,
        )?;
        let ConnectorProperties::Iceberg(properties) = config else {
            return Err(anyhow!("Iceberg metadata scan received a non-Iceberg connector").into());
        };

        Ok(Box::new(IcebergMetadataScanExecutor {
            schema: Schema::from_iter(node.columns.iter().map(Field::from)),
            properties: *properties,
            metadata_type,
            time_travel_info,
            filter: IcebergMetadataFilter {
                content: node.content_filter.clone(),
                manifest_path: node.manifest_path_filter.clone(),
            },
            output_indices,
            limit: node.limit,
            identity: source.plan_node().get_identity().clone(),
            chunk_size: source.context().get_config().developer.chunk_size,
        }))
    }
}
