use crate::error::{ErrorCode, Result, RwError};
use risingwave_pb::expr::agg_call::Type;
use risingwave_proto::expr::AggCall_Type;
use std::convert::TryFrom;

/// Kind of aggregation function
#[derive(Debug)]
pub enum AggKind {
    Min,
    Max,
    Sum,
    Count,
    RowCount,
    Avg,
}

impl TryFrom<AggCall_Type> for AggKind {
    type Error = RwError;

    fn try_from(proto: AggCall_Type) -> Result<Self> {
        match proto {
            AggCall_Type::MIN => Ok(AggKind::Min),
            AggCall_Type::MAX => Ok(AggKind::Max),
            AggCall_Type::SUM => Ok(AggKind::Sum),
            AggCall_Type::AVG => Ok(AggKind::Avg),
            AggCall_Type::COUNT => Ok(AggKind::Count),
            _ => Err(ErrorCode::InternalError("Unrecognized agg.".into()).into()),
        }
    }
}

impl TryFrom<risingwave_pb::expr::agg_call::Type> for AggKind {
    type Error = RwError;

    fn try_from(prost: risingwave_pb::expr::agg_call::Type) -> Result<Self> {
        match prost {
            Type::Min => Ok(AggKind::Min),
            Type::Max => Ok(AggKind::Max),
            Type::Sum => Ok(AggKind::Sum),
            Type::Avg => Ok(AggKind::Avg),
            Type::Count => Ok(AggKind::Count),
            _ => Err(ErrorCode::InternalError("Unrecognized agg.".into()).into()),
        }
    }
}
