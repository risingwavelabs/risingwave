use prost::Message;
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_pb::hummock::HummockVersion;

use crate::model::{MetadataModel, Transactional};

/// Column family name for hummock version.
/// `cf(hummock_version)`: `HummockVersionRefId` -> `HummockVersion`
const HUMMOCK_VERSION_CF_NAME: &str = "cf/hummock_version";

/// `HummockVersion` tracks `SSTables` in given version.
impl MetadataModel for HummockVersion {
    type ProstType = HummockVersion;
    type KeyType = HummockVersionRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_VERSION_CF_NAME)
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
        Ok(HummockVersionRefId { id: self.id })
    }
}

impl Transactional for HummockVersion {}
