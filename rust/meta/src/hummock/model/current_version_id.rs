use prost::Message;
use risingwave_common::error::Result;
use risingwave_pb::hummock::HummockVersionRefId;
use risingwave_storage::hummock::{HummockVersionId, FIRST_VERSION_ID};

use crate::hummock::model::HUMMOCK_DEFAULT_CF_NAME;
use crate::storage::{MetaStore, Transaction};

/// Hummock version id key.
/// `cf(hummock_default)`: `hummock_version_id_key` -> `HummockVersionRefId`
const HUMMOCK_VERSION_ID_KEY: &str = "version_id";

pub struct CurrentHummockVersionId {
    id: HummockVersionId,
}

impl CurrentHummockVersionId {
    pub fn new() -> CurrentHummockVersionId {
        CurrentHummockVersionId {
            id: FIRST_VERSION_ID,
        }
    }

    fn cf_name() -> &'static str {
        HUMMOCK_DEFAULT_CF_NAME
    }

    fn key() -> &'static str {
        HUMMOCK_VERSION_ID_KEY
    }

    pub async fn get<S: MetaStore>(meta_store_ref: &S) -> Result<CurrentHummockVersionId> {
        let byte_vec = meta_store_ref
            .get_cf(
                CurrentHummockVersionId::cf_name(),
                CurrentHummockVersionId::key().as_bytes(),
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
        trx.put(
            CurrentHummockVersionId::cf_name().to_string(),
            CurrentHummockVersionId::key().as_bytes().to_vec(),
            HummockVersionRefId { id: self.id }.encode_to_vec(),
        );
    }

    pub fn id(&self) -> HummockVersionId {
        self.id
    }
}
