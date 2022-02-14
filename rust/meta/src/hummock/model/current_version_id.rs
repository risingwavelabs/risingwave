use prost::Message;
use risingwave_common::error::Result;
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_storage::hummock::{HummockVersionId, FIRST_VERSION_ID};

use crate::hummock::model::HUMMOCK_DEFAULT_CF_NAME;
use crate::manager::SINGLE_VERSION_EPOCH;
use crate::storage::{ColumnFamilyUtils, MetaStore, Operation, Transaction};

/// Hummock version id key.
/// `cf(hummock_default)`: `hummock_version_id_key` -> `HummockVersionRefId`
const HUMMOCK_VERSION_ID_LEY: &str = "version_id";

pub struct CurrentHummockVersionId {
    id: HummockVersionId,
}

impl CurrentHummockVersionId {
    pub fn new() -> CurrentHummockVersionId {
        CurrentHummockVersionId {
            id: FIRST_VERSION_ID,
        }
    }

    pub async fn get(meta_store_ref: &dyn MetaStore) -> Result<CurrentHummockVersionId> {
        let byte_vec = meta_store_ref
            .get_cf(
                HUMMOCK_DEFAULT_CF_NAME,
                HUMMOCK_VERSION_ID_LEY.as_bytes(),
                SINGLE_VERSION_EPOCH,
            )
            .await?;
        let instant = CurrentHummockVersionId {
            id: HummockVersionRefId::decode(byte_vec.as_slice())?.id,
        };
        Ok(instant)
    }

    /// Increase version id, return previous one
    pub fn increase(&mut self) -> HummockVersionId {
        let previous_id = self.id;
        self.id += 1;
        previous_id
    }

    pub fn update_in_transaction(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(HUMMOCK_VERSION_ID_LEY, HUMMOCK_DEFAULT_CF_NAME),
            HummockVersionRefId { id: self.id }.encode_to_vec(),
            None,
        )]);
    }

    pub fn id(&self) -> HummockVersionId {
        self.id
    }
}
