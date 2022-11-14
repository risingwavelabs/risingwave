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

use std::convert::TryFrom;

use parse_display::{Display, FromStr};
use risingwave_common::bail;
use risingwave_pb::expr::agg_call::Type;

use crate::{ExprError, Result};

/// Kind of aggregation function
#[derive(Debug, Display, FromStr, Copy, Clone, PartialEq, Eq, Hash)]
#[display(style = "snake_case")]
pub enum AggKind {
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
}

impl TryFrom<Type> for AggKind {
    type Error = ExprError;

    fn try_from(prost: Type) -> Result<Self> {
        match prost {
            Type::Min => Ok(AggKind::Min),
            Type::Max => Ok(AggKind::Max),
            Type::Sum => Ok(AggKind::Sum),
            Type::Sum0 => Ok(AggKind::Sum0),
            Type::Avg => Ok(AggKind::Avg),
            Type::Count => Ok(AggKind::Count),
            Type::StringAgg => Ok(AggKind::StringAgg),
            Type::ApproxCountDistinct => Ok(AggKind::ApproxCountDistinct),
            Type::ArrayAgg => Ok(AggKind::ArrayAgg),
            Type::FirstValue => Ok(AggKind::FirstValue),
            Type::Unspecified => bail!("Unrecognized agg."),
        }
    }
}

impl AggKind {
    pub fn to_prost(self) -> Type {
        match self {
            Self::Min => Type::Min,
            Self::Max => Type::Max,
            Self::Sum => Type::Sum,
            Self::Sum0 => Type::Sum0,
            Self::Avg => Type::Avg,
            Self::Count => Type::Count,
            Self::StringAgg => Type::StringAgg,
            Self::ApproxCountDistinct => Type::ApproxCountDistinct,
            Self::ArrayAgg => Type::ArrayAgg,
            Self::FirstValue => Type::FirstValue,
        }
    }
}
