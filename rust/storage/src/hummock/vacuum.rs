use std::sync::Arc;

use risingwave_pb::hummock::VacuumTask;

use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::SstableStoreRef;

pub struct Vacuum {}

impl Vacuum {
    pub async fn vacuum(
        sstable_store_ref: SstableStoreRef,
        vacuum_task: VacuumTask,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> risingwave_common::error::Result<()> {
        let sst_ids = vacuum_task.sstable_ids;
        for sst_id in &sst_ids {
            sstable_store_ref
                .store()
                .delete(sstable_store_ref.get_sst_meta_path(*sst_id).as_str())
                .await?;
            sstable_store_ref
                .store()
                .delete(sstable_store_ref.get_sst_data_path(*sst_id).as_str())
                .await?;
        }

        // TODO: report progress instead of in one go.
        hummock_meta_client
            .report_vacuum_task(VacuumTask {
                sstable_ids: sst_ids,
            })
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_pb::hummock::VacuumTask;

    use crate::hummock::iterator::test_utils::default_builder_opt_for_test;
    use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
    use crate::hummock::test_utils::gen_default_test_sstable;
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
            let sstable = gen_default_test_sstable(
                default_builder_opt_for_test(),
                *sstable_id,
                sstable_store_ref.clone(),
            )
            .await;
            sstables.push(sstable);
        }

        // Delete all existent SSTs and a nonexistent SSTs. Trying to delete a nonexistent SST is
        // OK.
        let nonexistent_id = 11u64;
        let vacuum_task = VacuumTask {
            sstable_ids: sst_ids
                .into_iter()
                .chain(iter::once(nonexistent_id))
                .collect_vec(),
        };
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
