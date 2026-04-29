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

use std::collections::HashMap;

use risingwave_common::id::SinkId;
use risingwave_common::types::Fields;
use risingwave_connector::WithPropertiesExt;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwIcebergCompactionSchedules {
    #[primary_key]
    sink_id: SinkId,
    schema_name: Option<String>,
    sink_name: Option<String>,
    task_type: String,
    trigger_interval_sec: i64,
    trigger_snapshot_count: i64,
    schedule_state: String,
    next_compaction_after_sec: Option<i64>,
    pending_snapshot_count: Option<i64>,
    is_triggerable: bool,
}

#[system_catalog(table, "rw_catalog.rw_iceberg_compaction_schedules")]
async fn read(reader: &SysCatalogReaderImpl) -> Result<Vec<RwIcebergCompactionSchedules>> {
    let sink_name_by_id: HashMap<_, _> = {
        let catalog_reader = reader.catalog_reader.read_guard();
        let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;

        schemas
            .flat_map(|schema| {
                schema.iter_sink().filter_map(|sink| {
                    sink.properties
                        .is_iceberg_connector()
                        .then_some((sink.id, (schema.name.clone(), sink.name.clone())))
                })
            })
            .collect()
    };

    let statuses = reader.meta_client.list_iceberg_compaction_status().await?;
    Ok(statuses
        .into_iter()
        .map(|status| {
            let sink_id = SinkId::from(status.sink_id);
            let names = sink_name_by_id.get(&sink_id).cloned();
            RwIcebergCompactionSchedules {
                sink_id,
                schema_name: names.as_ref().map(|(schema_name, _)| schema_name.clone()),
                sink_name: names.map(|(_, sink_name)| sink_name),
                task_type: status.task_type,
                trigger_interval_sec: status.trigger_interval_sec as i64,
                trigger_snapshot_count: status.trigger_snapshot_count as i64,
                schedule_state: status.schedule_state,
                next_compaction_after_sec: status.next_compaction_after_sec.map(|v| v as i64),
                pending_snapshot_count: status.pending_snapshot_count.map(|v| v as i64),
                is_triggerable: status.is_triggerable,
            }
        })
        .collect())
}
