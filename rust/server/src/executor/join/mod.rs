use crate::executor::join::JoinType::Inner;

mod chunked_data;
use crate::risingwave_proto::plan::JoinType as JoinTypeProto;
mod hash_join;
mod hash_join_state;
pub mod nested_loop_join;

#[derive(Copy, Clone, Debug)]
pub(super) enum JoinType {
    Inner,
    LeftOuter,
    /// Semi join when probe side should output when matched
    LeftSemi,
    /// Anti join when probe side should not output when matched
    LeftAnti,
    RightOuter,
    /// Semi join when build side should output when matched
    RightSemi,
    /// Anti join when build side should output when matched
    RightAnti,
    FullOuter,
}

impl JoinType {
    #[inline(always)]
    pub(super) fn need_join_remaining(self) -> bool {
        matches!(
            self,
            JoinType::RightOuter | JoinType::RightAnti | JoinType::FullOuter | JoinType::RightSemi
        )
    }
}

impl Default for JoinType {
    fn default() -> Self {
        Inner
    }
}

impl JoinType {
    pub fn from_proto_join_type(join_type_proto: JoinTypeProto) -> Self {
        match join_type_proto {
            JoinTypeProto::INNER => JoinType::Inner,
            JoinTypeProto::LEFT_OUTER => JoinType::LeftOuter,
            JoinTypeProto::RIGHT_OUTER => JoinType::RightOuter,
            JoinTypeProto::FULL_OUTER => JoinType::FullOuter,
            // Join type should not distinguish Left/Right Semi/Anti.
            JoinTypeProto::SEMI => JoinType::LeftSemi,
            JoinTypeProto::ANTI => JoinType::LeftAnti,
        }
    }
}
