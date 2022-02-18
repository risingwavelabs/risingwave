use prost::Message;
use risingwave_common::error::Result;
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_storage::hummock::{HummockVersionId, FIRST_VERSION_ID};

use crate::hummock::model::HUMMOCK_DEFAULT_CF_NAME;
use crate::storage::{ColumnFamilyUtils, MetaStore, Operation, Transaction};

/// Hummock version id key.
/// `cf(hummock_default)`: `hummock_version_id_key` -> `HummockVersionRefId`
const HUMMOCK_VERSION_ID_LEY: &str = "version_id";

pub struct CurrentHummockVersionId {
    cf_ident: String,
    id: HummockVersionId,
}

impl CurrentHummockVersionId {
    pub fn new(cf_ident: &str) -> CurrentHummockVersionId {
        CurrentHummockVersionId {
            cf_ident: String::from(cf_ident),
            id: FIRST_VERSION_ID,
        }
    }

    fn cf_name() -> &'static str {
        HUMMOCK_DEFAULT_CF_NAME
    }

    fn key() -> &'static str {
        HUMMOCK_VERSION_ID_LEY
    }

    pub async fn get<S: MetaStore>(
        meta_store_ref: &S,
        cf_ident: &str,
    ) -> Result<CurrentHummockVersionId> {
        let byte_vec = meta_store_ref
            .get_cf(
                &ColumnFamilyUtils::get_composed_cf(CurrentHummockVersionId::cf_name(), cf_ident),
                CurrentHummockVersionId::key().as_bytes(),
            )
            .await?;
        let instant = CurrentHummockVersionId {
            cf_ident: String::from(cf_ident),
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
        trx.add_operations(vec![Operation::Put {
            cf: ColumnFamilyUtils::get_composed_cf(
                CurrentHummockVersionId::cf_name(),
                &self.cf_ident,
            ),
            key: CurrentHummockVersionId::key().as_bytes().to_vec(),
            value: HummockVersionRefId { id: self.id }.encode_to_vec(),
        }]);
    }

    pub fn id(&self) -> HummockVersionId {
        self.id
    }
}
