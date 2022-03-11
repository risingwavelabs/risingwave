use std::sync::Arc;

use risingwave_pb::hummock::vacuum_task::{Task, VaccumOrphanData, VacuumTrackedData};
use risingwave_pb::hummock::{vacuum_task, VacuumTask};

use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::SstableStoreRef;

pub struct Vacuum {}

impl Vacuum {
    pub async fn vacuum(
        sstable_store_ref: SstableStoreRef,
        vacuum_task: vacuum_task::Task,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> risingwave_common::error::Result<()> {
        match vacuum_task.clone() {
            Task::Tracked(VacuumTrackedData { sstable_ids })
            | Task::Orphan(VaccumOrphanData { sstable_ids }) => {
                for sstable_id in &sstable_ids {
                    sstable_store_ref
                        .store()
                        .delete(sstable_store_ref.get_sst_meta_path(*sstable_id).as_str())
                        .await?;
                    sstable_store_ref
                        .store()
                        .delete(sstable_store_ref.get_sst_data_path(*sstable_id).as_str())
                        .await?;
                }

                hummock_meta_client
                    .report_vacuum_task(VacuumTask {
                        task: Some(vacuum_task),
                    })
                    .await?;

                tracing::debug!(
                    "vacuum {} tracked SSTs, {:?}",
                    sstable_ids.len(),
                    sstable_ids
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_pb::hummock::vacuum_task;

    use crate::hummock::iterator::test_utils::{default_builder_opt_for_test, gen_test_sstable};
    use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
    use crate::hummock::vacuum::Vacuum;
    use crate::hummock::SstableStore;
    use crate::object::InMemObjectStore;

    #[tokio::test]
    async fn test_vacuum_tracked_data() {
        let sstable_store_ref = Arc::new(SstableStore::new(
            Arc::new(InMemObjectStore::new()),
            String::from("test_dir"),
        ));

        // Put some SSTs to object store
        let sst_ids = (1..10).collect_vec();
        let mut sstables = vec![];
        for sstable_id in &sst_ids {
            let sstable = gen_test_sstable(
                *sstable_id,
                default_builder_opt_for_test(),
                sstable_store_ref.clone(),
            )
            .await;
            sstables.push(sstable);
        }

        // Delete all existent SSTs and a nonexistent SSTs. Trying to delete a nonexistent SST is
        // OK.
        let nonexistent_id = 11u64;
        let vacuum_task = vacuum_task::Task::Tracked(vacuum_task::VacuumTrackedData {
            sstable_ids: sst_ids
                .into_iter()
                .chain(iter::once(nonexistent_id))
                .collect_vec(),
        });
        Vacuum::vacuum(
            sstable_store_ref,
            vacuum_task,
            Arc::new(MockHummockMetaClient::new(Arc::new(
                MockHummockMetaService::new(),
            ))),
        )
        .await
        .unwrap();
    }
}
