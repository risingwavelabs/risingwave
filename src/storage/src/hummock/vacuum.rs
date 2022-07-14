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

use risingwave_object_store::object::ObjectStore;
use risingwave_pb::hummock::VacuumTask;
use risingwave_rpc_client::HummockMetaClient;

use super::{HummockError, HummockResult};
use crate::hummock::SstableStoreRef;

pub struct Vacuum;

impl Vacuum {
    pub async fn vacuum(
        sstable_store: SstableStoreRef,
        vacuum_task: VacuumTask,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> HummockResult<()> {
        let store = sstable_store.store();
        let sst_ids = vacuum_task.sstable_ids;
        for sst_id in &sst_ids {
            // Meta
            store
                .delete(sstable_store.get_sst_meta_path(*sst_id).as_str())
                .await
                .map_err(HummockError::object_io_error)?;
            // Data
            store
                .delete(sstable_store.get_sst_data_path(*sst_id).as_str())
                .await
                .map_err(HummockError::object_io_error)?;
        }

        // TODO: report progress instead of in one go.
        hummock_meta_client
            .report_vacuum_task(VacuumTask {
                sstable_ids: sst_ids,
            })
            .await
            .map_err(|e| {
                HummockError::meta_error(format!("failed to report vacuum task: {e:?}"))
            })?;

        Ok(())
    }
}
