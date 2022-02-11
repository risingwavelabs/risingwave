use prost::Message;
use risingwave_pb::hummock::{SstableInfo, SstableRefId};

use crate::hummock::model::Transactional;
use crate::model::MetadataModel;
use crate::storage::{ColumnFamilyUtils, Operation, Transaction};

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

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost
    }

    fn key(&self) -> risingwave_common::error::Result<Self::KeyType> {
        Ok(SstableRefId { id: self.id })
    }
}

impl Transactional for SstableInfo {
    fn upsert(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Put(
            ColumnFamilyUtils::prefix_key_with_cf(
                &self.key().unwrap().encode_to_vec(),
                SstableInfo::cf_name(),
            ),
            self.encode_to_vec(),
            None,
        )]);
    }

    fn delete(&self, trx: &mut Transaction) {
        trx.add_operations(vec![Operation::Delete(
            ColumnFamilyUtils::prefix_key_with_cf(
                &self.key().unwrap().encode_to_vec(),
                SstableInfo::cf_name(),
            ),
            None,
        )]);
    }
}
