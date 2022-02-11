use prost::Message;
use risingwave_pb::hummock::{HummockContextPinnedSnapshot, HummockContextRefId};
use risingwave_storage::hummock::HummockEpoch;

use crate::model::{MetadataModel, Transactional};
use crate::storage::Transaction;

/// Column family name for hummock context pinned snapshot
/// `cf(hummock_context_pinned_snapshot)`: `HummockContextRefId` -> `HummockContextPinnedSnapshot`
const HUMMOCK_CONTEXT_PINNED_SNAPSHOT_CF_NAME: &str = "cf/hummock_context_pinned_snapshot";

impl MetadataModel for HummockContextPinnedSnapshot {
    type ProstType = HummockContextPinnedSnapshot;
    type KeyType = HummockContextRefId;

    fn cf_name() -> String {
        String::from(HUMMOCK_CONTEXT_PINNED_SNAPSHOT_CF_NAME)
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

pub trait HummockContextPinnedSnapshotExt {
    fn pin_snapshot(&mut self, new_snapshot_id: HummockEpoch);

    fn unpin_snapshot(&mut self, pinned_snapshot_id: HummockEpoch);

    fn update_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()>;
}

impl HummockContextPinnedSnapshotExt for HummockContextPinnedSnapshot {
    fn pin_snapshot(&mut self, epoch: HummockEpoch) {
        let found = self.snapshot_id.iter().position(|&v| v == epoch);
        if found.is_none() {
            self.snapshot_id.push(epoch);
        }
    }

    fn unpin_snapshot(&mut self, epoch: HummockEpoch) {
        let found = self.snapshot_id.iter().position(|&v| v == epoch);
        if let Some(pos) = found {
            self.snapshot_id.remove(pos);
        }
    }

    fn update_in_transaction(&self, trx: &mut Transaction) -> risingwave_common::error::Result<()> {
        if self.snapshot_id.is_empty() {
            self.delete_in_transaction(trx)?;
        } else {
            self.upsert_in_transaction(trx)?;
        }
        Ok(())
    }
}

impl Transactional for HummockContextPinnedSnapshot {}
