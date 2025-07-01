// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_expr::bail;
use risingwave_pb::plan_common::{AsOfJoinDesc, AsOfJoinInequalityType};

use crate::error::StreamResult;

pub mod builder;
pub mod hash_join;
pub mod join_row_set;
pub mod row;

pub(crate) type JoinOpPrimitive = bool;

#[allow(non_snake_case, non_upper_case_globals)]
pub(crate) mod JoinOp {
    use super::JoinOpPrimitive;

    pub const Insert: JoinOpPrimitive = true;
    pub const Delete: JoinOpPrimitive = false;
}

pub type JoinEncodingPrimitive = u8;

#[allow(non_snake_case, non_upper_case_globals)]
pub mod JoinEncoding {
    use super::JoinEncodingPrimitive;
    pub const Memory: JoinEncodingPrimitive = 0;

    pub const Cpu: JoinEncodingPrimitive = 1;
}

/// The `JoinType` and `SideType` are to mimic a enum, because currently
/// enum is not supported in const generic.
// TODO: Use enum to replace this once [feature(adt_const_params)](https://github.com/rust-lang/rust/issues/95174) get completed.
pub type JoinTypePrimitive = u8;

#[allow(non_snake_case, non_upper_case_globals)]
pub mod JoinType {
    use super::JoinTypePrimitive;
    pub const Inner: JoinTypePrimitive = 0;
    pub const LeftOuter: JoinTypePrimitive = 1;
    pub const RightOuter: JoinTypePrimitive = 2;
    pub const FullOuter: JoinTypePrimitive = 3;
    pub const LeftSemi: JoinTypePrimitive = 4;
    pub const LeftAnti: JoinTypePrimitive = 5;
    pub const RightSemi: JoinTypePrimitive = 6;
    pub const RightAnti: JoinTypePrimitive = 7;
}

pub type AsOfJoinTypePrimitive = u8;

#[allow(non_snake_case, non_upper_case_globals)]
pub mod AsOfJoinType {
    use super::AsOfJoinTypePrimitive;
    pub const Inner: AsOfJoinTypePrimitive = 0;
    pub const LeftOuter: AsOfJoinTypePrimitive = 1;
}

pub type SideTypePrimitive = u8;
#[allow(non_snake_case, non_upper_case_globals)]
pub mod SideType {
    use super::SideTypePrimitive;
    pub const Left: SideTypePrimitive = 0;
    pub const Right: SideTypePrimitive = 1;
}

pub enum AsOfInequalityType {
    Le,
    Lt,
    Ge,
    Gt,
}

pub struct AsOfDesc {
    pub left_idx: usize,
    pub right_idx: usize,
    pub inequality_type: AsOfInequalityType,
}

impl AsOfDesc {
    pub fn from_protobuf(desc_proto: &AsOfJoinDesc) -> StreamResult<Self> {
        let typ = match desc_proto.inequality_type() {
            AsOfJoinInequalityType::AsOfInequalityTypeLt => AsOfInequalityType::Lt,
            AsOfJoinInequalityType::AsOfInequalityTypeLe => AsOfInequalityType::Le,
            AsOfJoinInequalityType::AsOfInequalityTypeGt => AsOfInequalityType::Gt,
            AsOfJoinInequalityType::AsOfInequalityTypeGe => AsOfInequalityType::Ge,
            AsOfJoinInequalityType::AsOfInequalityTypeUnspecified => {
                bail!("unspecified AsOf join inequality type")
            }
        };
        Ok(Self {
            left_idx: desc_proto.left_idx as usize,
            right_idx: desc_proto.right_idx as usize,
            inequality_type: typ,
        })
    }
}

pub const fn is_outer_side(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Left)
        || (join_type == JoinType::RightOuter && side_type == SideType::Right)
}

pub const fn outer_side_null(join_type: JoinTypePrimitive, side_type: SideTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || (join_type == JoinType::LeftOuter && side_type == SideType::Right)
        || (join_type == JoinType::RightOuter && side_type == SideType::Left)
}

/// Send the update only once if the join type is semi/anti and the update is the same side as the
/// join
pub const fn forward_exactly_once(
    join_type: JoinTypePrimitive,
    side_type: SideTypePrimitive,
) -> bool {
    ((join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti)
        && side_type == SideType::Left)
        || ((join_type == JoinType::RightSemi || join_type == JoinType::RightAnti)
            && side_type == SideType::Right)
}

pub const fn only_forward_matched_side(
    join_type: JoinTypePrimitive,
    side_type: SideTypePrimitive,
) -> bool {
    ((join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti)
        && side_type == SideType::Right)
        || ((join_type == JoinType::RightSemi || join_type == JoinType::RightAnti)
            && side_type == SideType::Left)
}

pub const fn is_semi(join_type: JoinTypePrimitive) -> bool {
    join_type == JoinType::LeftSemi || join_type == JoinType::RightSemi
}

pub const fn is_anti(join_type: JoinTypePrimitive) -> bool {
    join_type == JoinType::LeftAnti || join_type == JoinType::RightAnti
}

pub const fn is_left_semi_or_anti(join_type: JoinTypePrimitive) -> bool {
    join_type == JoinType::LeftSemi || join_type == JoinType::LeftAnti
}

pub const fn is_right_semi_or_anti(join_type: JoinTypePrimitive) -> bool {
    join_type == JoinType::RightSemi || join_type == JoinType::RightAnti
}

pub const fn need_left_degree(join_type: JoinTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || join_type == JoinType::LeftOuter
        || join_type == JoinType::LeftAnti
        || join_type == JoinType::LeftSemi
}

pub const fn need_right_degree(join_type: JoinTypePrimitive) -> bool {
    join_type == JoinType::FullOuter
        || join_type == JoinType::RightOuter
        || join_type == JoinType::RightAnti
        || join_type == JoinType::RightSemi
}

pub const fn is_as_of_left_outer(join_type: AsOfJoinTypePrimitive) -> bool {
    join_type == AsOfJoinType::LeftOuter
}
