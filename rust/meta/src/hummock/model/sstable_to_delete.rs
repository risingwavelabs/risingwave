use prost::Message;
use risingwave_pb::hummock::hummock_version::HummockVersionRefId;
use risingwave_pb::hummock::HummockTablesToDelete;

use crate::hummock::model::Transactional;
use crate::model::MetadataModel;
use crate::storage::{ColumnFamilyUtils, Operation, Transaction};

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

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> risingwave_common::error::Result<Self::KeyType> {
        Ok(HummockVersionRefId {
            id: self.version_id,
        })
    }
}

impl Transactional for HummockTablesToDelete {
    fn upsert(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                &self.key().unwrap().encode_to_vec(),
                HummockTablesToDelete::cf_name(),
            ),
            self.encode_to_vec(),
            None,
        )]);
    }

    fn delete(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Delete(
            ColumnFamilyUtils::prefix_key_with_cf(
                &self.key().unwrap().encode_to_vec(),
                HummockTablesToDelete::cf_name(),
            ),
            None,
        )]);
    }
}
