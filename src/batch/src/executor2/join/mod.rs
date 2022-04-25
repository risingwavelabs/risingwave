mod chunked_data;
pub mod hash_join;
mod hash_join_state;

pub use chunked_data::*;
pub use hash_join::*;
use risingwave_pb::plan_common::JoinType as JoinTypeProst;

use crate::executor2::join::JoinType::Inner;
#[derive(Copy, Clone, Debug, PartialEq)]
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

    fn need_build(self) -> bool {
        match self {
            JoinType::RightSemi => true,
            other => other.need_join_remaining(),
        }
    }

    fn need_probe(self) -> bool {
        matches!(
            self,
            JoinType::FullOuter | JoinType::LeftOuter | JoinType::LeftAnti | JoinType::LeftSemi
        )
    }

    fn keep_all(self) -> bool {
        matches!(
            self,
            JoinType::FullOuter | JoinType::LeftOuter | JoinType::RightOuter | JoinType::Inner
        )
    }

    fn keep_left(self) -> bool {
        matches!(self, JoinType::LeftAnti | JoinType::LeftSemi)
    }

    fn keep_right(self) -> bool {
        matches!(self, JoinType::RightAnti | JoinType::RightSemi)
    }
}

impl Default for JoinType {
    fn default() -> Self {
        Inner
    }
}
