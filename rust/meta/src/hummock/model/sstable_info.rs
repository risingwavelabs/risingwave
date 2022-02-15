use prost::Message;
use risingwave_pb::hummock::{SstableInfo, SstableRefId};

use crate::model::{MetadataModel, MetadataUserCfModel, Transactional, TransactionalUserCf};

/// Column family name for hummock table.
/// `cf(hummock_sstable_info)`: `SstableRefId` -> `SstableInfo`
const HUMMOCK_TABLE_CF_NAME: &str = "cf/hummock_table";

impl MetadataModel for SstableInfo {
    type ProstType = SstableInfo;
    type KeyType = SstableRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_TABLE_CF_NAME)
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

impl MetadataUserCfModel for SstableInfo {}

impl Transactional for SstableInfo {}

impl TransactionalUserCf for SstableInfo {}
