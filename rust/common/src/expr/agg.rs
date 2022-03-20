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
//
use std::convert::TryFrom;

use risingwave_pb::expr::agg_call::Type;

use crate::error::{ErrorCode, Result, RwError};

/// Kind of aggregation function
#[derive(Debug, Clone, PartialEq)]
pub enum AggKind {
    Min,
    Max,
    Sum,
    Count,
    RowCount,
    Avg,
    StringAgg,
    SingleValue,
}

impl TryFrom<Type> for AggKind {
    type Error = RwError;

    fn try_from(prost: Type) -> Result<Self> {
        match prost {
            Type::Min => Ok(AggKind::Min),
            Type::Max => Ok(AggKind::Max),
            Type::Sum => Ok(AggKind::Sum),
            Type::Avg => Ok(AggKind::Avg),
            Type::Count => Ok(AggKind::Count),
            Type::StringAgg => Ok(AggKind::StringAgg),
            Type::SingleValue => Ok(AggKind::SingleValue),
            _ => Err(ErrorCode::InternalError("Unrecognized agg.".into()).into()),
        }
    }
}
