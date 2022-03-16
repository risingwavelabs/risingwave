use risingwave_common::error::Result;
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_storage::hummock::{HummockVersionId, FIRST_VERSION_ID};

use crate::hummock::model::HUMMOCK_DEFAULT_CF_NAME;
use crate::model::{MetadataModel, Transactional};
use crate::storage::MetaStore;

/// Hummock current version id key.
/// `cf(hummock_default)`: `hummock_version_id_key` -> `HummockVersionRefId`
const HUMMOCK_VERSION_ID_KEY: &str = "current_version_id";

/// `CurrentHummockVersionId` tracks the current version id.
#[derive(Clone, Debug, PartialEq)]
pub struct CurrentHummockVersionId {
    id: HummockVersionId,
}

impl MetadataModel for CurrentHummockVersionId {
    type ProstType = HummockVersionRefId;
    type KeyType = String;

    fn cf_name() -> String {
        HUMMOCK_DEFAULT_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        HummockVersionRefId { id: self.id }
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self { id: prost.id }
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(HUMMOCK_VERSION_ID_KEY.to_string())
    }
}

impl Transactional for CurrentHummockVersionId {}

impl CurrentHummockVersionId {
    pub fn new() -> CurrentHummockVersionId {
        CurrentHummockVersionId {
            id: FIRST_VERSION_ID,
        }
    }

    pub async fn get<S: MetaStore>(meta_store_ref: &S) -> Result<Option<CurrentHummockVersionId>> {
        CurrentHummockVersionId::select(meta_store_ref, &HUMMOCK_VERSION_ID_KEY.to_string()).await
    }

    /// Increase version id, return previous one
    pub fn increase(&mut self) -> HummockVersionId {
        let previous_id = self.id;
        self.id += 1;
        previous_id
    }

    pub fn id(&self) -> HummockVersionId {
        self.id
    }
}
