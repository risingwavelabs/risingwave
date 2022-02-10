use prost::Message;
use risingwave_pb::hummock::{HummockContextPinnedVersion, HummockContextRefId};
use risingwave_storage::hummock::HummockVersionId;

use crate::hummock::model::Transactional;
use crate::model::MetadataModel;
use crate::storage::{ColumnFamilyUtils, Operation, Transaction};

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
    fn update(&self, trx: &mut Transaction);
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

    fn update(&self, trx: &mut Transaction) {
        if self.version_id.is_empty() {
            self.delete(trx);
        } else {
            self.upsert(trx);
        }
    }
}

impl Transactional for HummockContextPinnedVersion {
    fn upsert(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                &self.key().unwrap().encode_to_vec(),
                HummockContextPinnedVersion::cf_name(),
            ),
            self.encode_to_vec(),
            None,
        )]);
    }

    fn delete(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Delete(
            ColumnFamilyUtils::prefix_key_with_cf(
                &self.key().unwrap().encode_to_vec(),
                HummockContextPinnedVersion::cf_name(),
            ),
            None,
        )]);
    }
}
