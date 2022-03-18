#![allow(dead_code)]

use risingwave_common::error::Result;
use risingwave_pb::common::HashMapping;

use super::MetadataModel;
use crate::cluster::ParallelUnitId;

/// `VirtualKey` is the logical key for consistent hash. One `VirtualKey` corresponds to exactly
/// one `ParallelUnit` or `Actor`, while a `ParallelUnit` or `Actor` can correspond to a number
/// of `VirtualKey`s.
pub type VirtualKey = usize;

/// Column family name for hash mapping.
const HASH_MAPPING_CF_NAME: &str = "cf/hash_mapping";
/// Hardcoded key for mapping storage.
const HASH_MAPPING_KEY: &str = "consistent_hash_mapping";

/// `ConsistentHashMapping` stores the hash mapping from `VirtualKey` to `ParallelUnitId` or
/// `ActorId` based on consistent hash, which serves for load balance of the cluster.
/// Specifically, `Dispatcher`dispatches compute tasks to downstream actors in a load balanced
/// way according to the mapping. When the mapping changes, every compute node in the cluster
/// should be informed.
#[derive(Debug, Clone)]
pub struct ConsistentHashMapping(HashMapping);

impl MetadataModel for ConsistentHashMapping {
    type ProstType = HashMapping;
    type KeyType = String;

    fn cf_name() -> String {
        HASH_MAPPING_CF_NAME.to_string()
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.0.clone()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        Self(prost)
    }

    fn key(&self) -> risingwave_common::error::Result<Self::KeyType> {
        Ok(HASH_MAPPING_KEY.to_string())
    }
}

impl ConsistentHashMapping {
    pub fn new() -> Self {
        Self(HashMapping {
            hash_mapping: Vec::new(),
        })
    }

    pub fn update_mapping(
        &mut self,
        virtual_key: VirtualKey,
        parallel_unit_id: ParallelUnitId,
    ) -> Result<ParallelUnitId> {
        assert!(
            virtual_key < self.0.get_hash_mapping().len(),
            "Cannot update virtual key {} because there are only {} slots.",
            virtual_key,
            self.0.get_hash_mapping().len()
        );
        let old_id = self.0.hash_mapping[virtual_key];
        self.0.hash_mapping[virtual_key] = parallel_unit_id;
        Ok(old_id)
    }

    pub fn set_mapping(&mut self, parallel_unit_ids: Vec<ParallelUnitId>) -> Result<()> {
        self.0.hash_mapping = parallel_unit_ids;
        Ok(())
    }

    pub fn get_mapping(&self) -> Vec<ParallelUnitId> {
        self.0.hash_mapping.clone()
    }

    pub fn clear_mapping(&mut self) {
        self.0.hash_mapping.clear();
    }
}
