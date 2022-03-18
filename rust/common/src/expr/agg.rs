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
