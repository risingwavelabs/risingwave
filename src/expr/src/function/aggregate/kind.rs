// Copyright 2023 RisingWave Labs
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

use parse_display::{Display, FromStr};
use risingwave_common::bail;
use risingwave_pb::expr::agg_call::PbType;

use crate::Result;

/// Kind of aggregation function
#[derive(Debug, Display, FromStr, Copy, Clone, PartialEq, Eq, Hash)]
#[display(style = "snake_case")]
pub enum AggKind {
    BitAnd,
    BitOr,
    BitXor,
    Min,
    Max,
    Sum,
    Sum0,
    Count,
    Avg,
    StringAgg,
    ApproxCountDistinct,
    ArrayAgg,
    FirstValue,
    VarPop,
    VarSamp,
    StddevPop,
    StddevSamp,
}

impl AggKind {
    pub fn from_protobuf(pb_type: PbType) -> Result<Self> {
        match pb_type {
            PbType::BitAnd => Ok(AggKind::BitAnd),
            PbType::BitOr => Ok(AggKind::BitOr),
            PbType::BitXor => Ok(AggKind::BitXor),
            PbType::Min => Ok(AggKind::Min),
            PbType::Max => Ok(AggKind::Max),
            PbType::Sum => Ok(AggKind::Sum),
            PbType::Sum0 => Ok(AggKind::Sum0),
            PbType::Avg => Ok(AggKind::Avg),
            PbType::Count => Ok(AggKind::Count),
            PbType::StringAgg => Ok(AggKind::StringAgg),
            PbType::ApproxCountDistinct => Ok(AggKind::ApproxCountDistinct),
            PbType::ArrayAgg => Ok(AggKind::ArrayAgg),
            PbType::FirstValue => Ok(AggKind::FirstValue),
            PbType::StddevPop => Ok(AggKind::StddevPop),
            PbType::StddevSamp => Ok(AggKind::StddevSamp),
            PbType::VarPop => Ok(AggKind::VarPop),
            PbType::VarSamp => Ok(AggKind::VarSamp),
            PbType::Unspecified => bail!("Unrecognized agg."),
        }
    }

    pub fn to_protobuf(self) -> PbType {
        match self {
            Self::BitAnd => PbType::BitAnd,
            Self::BitOr => PbType::BitOr,
            Self::BitXor => PbType::BitXor,
            Self::Min => PbType::Min,
            Self::Max => PbType::Max,
            Self::Sum => PbType::Sum,
            Self::Sum0 => PbType::Sum0,
            Self::Avg => PbType::Avg,
            Self::Count => PbType::Count,
            Self::StringAgg => PbType::StringAgg,
            Self::ApproxCountDistinct => PbType::ApproxCountDistinct,
            Self::ArrayAgg => PbType::ArrayAgg,
            Self::FirstValue => PbType::FirstValue,
            Self::StddevPop => PbType::StddevPop,
            Self::StddevSamp => PbType::StddevSamp,
            Self::VarPop => PbType::VarPop,
            Self::VarSamp => PbType::VarSamp,
        }
    }
}
