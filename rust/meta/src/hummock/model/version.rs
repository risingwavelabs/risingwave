use prost::Message;
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_pb::hummock::HummockVersion;

use crate::hummock::model::Transactional;
use crate::model::MetadataModel;
use crate::storage::{ColumnFamilyUtils, Operation, Transaction};

/// Column family name for hummock version.
/// `cf(hummock_version)`: `HummockVersionRefId` -> `HummockVersion`
const HUMMOCK_VERSION_CF_NAME: &str = "cf/hummock_version";

impl MetadataModel for HummockVersion {
    type ProstType = HummockVersion;
    type KeyType = HummockVersionRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_VERSION_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> risingwave_common::error::Result<Self::KeyType> {
        Ok(HummockVersionRefId { id: self.id })
    }
}

impl Transactional for HummockVersion {
    fn upsert(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                self.key().unwrap().encode_to_vec(),
                HummockVersion::cf_name(),
            ),
            self.encode_to_vec(),
            None,
        )]);
    }

    fn delete(&self, _trx: &mut Transaction) {
        todo!()
    }
}
