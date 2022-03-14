use prost::Message;
use risingwave_pb::hummock::{HummockContextRefId, HummockPinnedVersion};
use risingwave_storage::hummock::HummockVersionId;

use crate::model::{MetadataModel, Transactional};

/// Column family name for hummock pinned version
/// `cf(hummock_pinned_version)`: `HummockContextRefId` -> `HummockPinnedVersion`
const HUMMOCK_PINNED_VERSION_CF_NAME: &str = "cf/hummock_pinned_version";

impl MetadataModel for HummockPinnedVersion {
    type ProstType = HummockPinnedVersion;
    type KeyType = HummockContextRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_PINNED_VERSION_CF_NAME)
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

pub trait HummockPinnedVersionExt {
    fn pin_version(&mut self, version_id: HummockVersionId);
    fn unpin_version(&mut self, version_id: HummockVersionId);
}

impl HummockPinnedVersionExt for HummockPinnedVersion {
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
}

impl Transactional for HummockPinnedVersion {}
