use std::time::SystemTime;

use prost::Message;
use risingwave_pb::hummock::{SstableIdInfo, SstableRefId};

use crate::model::{MetadataModel, Transactional};

/// Column family name for hummock sstable id.
/// `cf(hummock_sstable_id)`: `SstableRefId` -> `SstableIdInfo`
const HUMMOCK_SSTABLE_ID_CF_NAME: &str = "cf/hummock_sstable_id";

pub const INVALID_TIMESTAMP: u64 = 0;

/// `SstableIdInfo` tracks when the sstable id is acquired from meta node and when the corresponding
/// sstable is tracked in meta node.
impl MetadataModel for SstableIdInfo {
    type ProstType = SstableIdInfo;
    type KeyType = SstableRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_SSTABLE_ID_CF_NAME)
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
        Ok(SstableRefId { id: self.id })
    }
}

impl Transactional for SstableIdInfo {}

pub fn get_timestamp_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
