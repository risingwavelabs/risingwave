// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod chunked_data;
pub mod hash_join;
mod hash_join_state;
pub mod nested_loop_join;
mod row_level_iter;
mod sort_merge_join;

pub use chunked_data::*;
pub use hash_join::*;
pub use nested_loop_join::*;
use risingwave_pb::plan_common::JoinType as JoinTypeProst;
pub use sort_merge_join::*;

use crate::executor::join::JoinType::Inner;
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
