use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::common::{HashMapping, ParallelUnit};

use super::MetadataModel;
use crate::cluster::ParallelUnitId;

pub type VirtualKey = usize;

/// Column family name for hash mapping.
const HASH_MAPPING_CF_NAME: &str = "cf/hash_mapping";
/// Hardcoded key for mapping storage.
const HASH_MAPPING_KEY: &str = "consistent_hash_mapping";

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

impl From<&Vec<ParallelUnitId>> for ConsistentHashMapping {
    fn from(mapping: &Vec<ParallelUnitId>) -> Self {
        let parallel_units: Vec<ParallelUnit> =
            mapping.iter().map(|&id| ParallelUnit { id }).collect();
        Self(HashMapping { parallel_units })
    }
}

impl ConsistentHashMapping {
    pub fn update(
        &mut self,
        virtual_key: VirtualKey,
        parallel_unit_id: ParallelUnitId,
    ) -> Result<ParallelUnitId> {
        assert!(
            virtual_key < self.0.get_parallel_units().len(),
            "Cannot update virtual key {} because there are only {} slots.",
            virtual_key,
            self.0.get_parallel_units().len()
        );
        let old_id = self.0.parallel_units[virtual_key].id;
        self.0.parallel_units[virtual_key] = ParallelUnit {
            id: parallel_unit_id,
        };
        Ok(old_id)
    }

    pub fn set_mapping(&mut self, parallel_unit_ids: Vec<ParallelUnitId>) -> Result<()> {
        self.0.parallel_units = parallel_unit_ids
            .into_iter()
            .map(|id| ParallelUnit { id })
            .collect_vec();
        Ok(())
    }

    pub fn get_mapping(&self) -> Vec<ParallelUnitId> {
        self
            .0
            .parallel_units
            .iter()
            .map(|parallel_unit| parallel_unit.id)
            .collect()
    }
}
