use prost::Message;
use risingwave_pb::hummock::{HummockContextPinnedVersion, HummockContextRefId};
use risingwave_storage::hummock::HummockVersionId;

use crate::model::{MetadataModel, Transactional};
use crate::storage::Transaction;

/// Column family name for hummock context pinned version
/// `cf(hummock_context_pinned_version)`: `HummockContextRefId` -> `HummockContextPinnedVersion`
const HUMMOCK_CONTEXT_PINNED_VERSION_CF_NAME: &str = "cf/hummock_context_pinned_version";

impl MetadataModel for HummockContextPinnedVersion {
    type ProstType = HummockContextPinnedVersion;
    type KeyType = HummockContextRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_CONTEXT_PINNED_VERSION_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.clone()
    }

    fn to_protobuf_encoded_vec(&self) -> Vec<u8> {
        self.encode_to_vec()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> risingwave_common::error::Result<Self::KeyType> {
        Ok(HummockContextRefId {
            id: self.context_id,
        })
    }
}

pub trait HummockContextPinnedVersionExt {
    fn pin_version(&mut self, version_id: HummockVersionId);
    fn unpin_version(&mut self, version_id: HummockVersionId);
    fn update_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()>;
}

impl HummockContextPinnedVersionExt for HummockContextPinnedVersion {
    fn pin_version(&mut self, version_id: HummockVersionId) {
        let found = self.version_id.iter().position(|&v| v == version_id);
        if found.is_none() {
            self.version_id.push(version_id);
        }
    }

    fn unpin_version(&mut self, pinned_version_id: HummockVersionId) {
        let found = self.version_id.iter().position(|&v| v == pinned_version_id);
        if let Some(pos) = found {
            self.version_id.remove(pos);
        }
    }

    fn update_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()> {
        if self.version_id.is_empty() {
            self.delete_in_transaction(trx)?;
        } else {
            self.upsert_in_transaction(trx)?;
        }
        Ok(())
    }
}

impl Transactional for HummockContextPinnedVersion {}
