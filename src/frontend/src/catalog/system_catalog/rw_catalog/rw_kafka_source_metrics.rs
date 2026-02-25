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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
#[primary_key(source_id, partition_id)]
struct RwKafkaSourceMetric {
    source_id: i32,
    partition_id: String,
    high_watermark: Option<i64>,
    latest_offset: Option<i64>,
}

#[system_catalog(table, "rw_catalog.rw_kafka_source_metrics")]
async fn read_rw_kafka_source_metrics(
    reader: &SysCatalogReaderImpl,
) -> Result<Vec<RwKafkaSourceMetric>> {
    let metrics = reader.metrics_reader.get_kafka_source_metrics().await?;
    Ok(metrics
        .into_iter()
        .map(|metric| RwKafkaSourceMetric {
            source_id: metric.source_id as i32,
            partition_id: metric.partition_id,
            high_watermark: metric.high_watermark,
            latest_offset: metric.latest_offset,
        })
        .collect())
}
