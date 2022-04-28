use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_pb::hummock::SstableInfo;

pub trait OverlapStrategy: Send + Sync {
    fn check_overlap(&self, a: &SstableInfo, b: &SstableInfo) -> bool;
}

#[derive(Default)]
pub struct RangeOverlapStrategy {}

impl OverlapStrategy for RangeOverlapStrategy {
    fn check_overlap(&self, a: &SstableInfo, b: &SstableInfo) -> bool {
        let key_range1 = KeyRange::from(a.key_range.as_ref().unwrap());
        let key_range2 = KeyRange::from(b.key_range.as_ref().unwrap());
        key_range1.full_key_overlap(&key_range2)
    }
}
