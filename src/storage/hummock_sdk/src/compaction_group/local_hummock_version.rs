use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::hummock_version_delta::GroupDeltas;
use risingwave_pb::hummock::{
    CompactionConfig, GroupConstruct, GroupDestroy, HummockVersion, HummockVersionDelta, Level,
    LevelType, OverlappingLevel, SstableInfo,
};

pub struct ZeroLevel {

}

pub struct LocalLevels {
    pub levels: Vec<Level>,
    pub l0: ZeroLevel,
}

#[derive(Clone, PartialEq)]
pub struct LocalHummockVersion {
    pub id: u64,
    pub levels: std::collections::HashMap<u64, LocalLevels>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
}