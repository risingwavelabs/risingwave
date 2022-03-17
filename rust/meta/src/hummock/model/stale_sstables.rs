use prost::Message;
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_pb::hummock::HummockStaleSstables;

use crate::model::{MetadataModel, Transactional};

/// Column family name for stale hummock sstables.
/// `cf(hummock_stale_sstables)`: `HummockVersionRefId` -> `HummockStaleSstables`
const HUMMOCK_STALE_SSTABLES_CF_NAME: &str = "cf/hummock_stale_sstables";

/// `HummockStaleSstables` tracks `SSTables` no longer needed after the given version.
impl MetadataModel for HummockStaleSstables {
    type ProstType = HummockStaleSstables;
    type KeyType = HummockVersionRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_STALE_SSTABLES_CF_NAME)
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

impl Transactional for HummockStaleSstables {}
