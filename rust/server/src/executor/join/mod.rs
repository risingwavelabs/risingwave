use crate::executor::join::JoinType::Inner;
use risingwave_pb::plan::JoinType as JoinTypeProst;
use risingwave_proto::plan::JoinType as JoinTypeProto;

mod chunked_data;
mod hash_join;
pub use hash_join::*;

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
            JoinType::RightOuter | JoinType::RightAnti | JoinType::FullOuter
        )
    }

    pub fn from_prost(prost: JoinTypeProst) -> Self {
        match prost {
            JoinTypeProst::Inner => JoinType::Inner,
            JoinTypeProst::LeftOuter => JoinType::LeftOuter,
            JoinTypeProst::LeftSemi => JoinType::LeftSemi,
            JoinTypeProst::LeftAnti => JoinType::LeftAnti,
            JoinTypeProst::RightOuter => JoinType::RightOuter,
            JoinTypeProst::RightSemi => JoinType::RightSemi,
            JoinTypeProst::RightAnti => JoinType::RightAnti,
            JoinTypeProst::FullOuter => JoinType::FullOuter,
        }
    }

    pub fn from_proto(proto: JoinTypeProto) -> Self {
        match proto {
            JoinTypeProto::INNER => JoinType::Inner,
            JoinTypeProto::LEFT_OUTER => JoinType::LeftOuter,
            JoinTypeProto::LEFT_SEMI => JoinType::LeftSemi,
            JoinTypeProto::LEFT_ANTI => JoinType::LeftAnti,
            JoinTypeProto::RIGHT_OUTER => JoinType::RightOuter,
            JoinTypeProto::RIGHT_SEMI => JoinType::RightSemi,
            JoinTypeProto::RIGHT_ANTI => JoinType::RightAnti,
            JoinTypeProto::FULL_OUTER => JoinType::FullOuter,
        }
    }
}

impl Default for JoinType {
    fn default() -> Self {
        Inner
    }
}
