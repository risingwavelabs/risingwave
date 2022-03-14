use prost::Message;
use risingwave_pb::hummock::{HummockTablesToDelete, HummockVersionRefId};

use crate::model::{MetadataModel, Transactional};

/// Column family name for hummock deletion.
/// `cf(hummock_sstable_to_delete)`: `HummockVersionRefId` -> `HummockTablesToDelete`
const HUMMOCK_DELETION_CF_NAME: &str = "cf/hummock_sstable_to_delete";

impl MetadataModel for HummockTablesToDelete {
    type ProstType = HummockTablesToDelete;
    type KeyType = HummockVersionRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_DELETION_CF_NAME)
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
        Ok(HummockVersionRefId {
            id: self.version_id,
        })
    }
}

impl Transactional for HummockTablesToDelete {}
