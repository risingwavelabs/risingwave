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
use std::sync::Arc;

use risingwave_common::bail;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_expr::expr::{build_from_prost, AggKind};
use risingwave_pb::plan_common::OrderType as ProstOrderType;

use super::*;
use crate::executor::aggregation::{AggArgs, AggCall};

pub fn build_agg_call_from_prost(
    append_only: bool,
    agg_call_proto: &risingwave_pb::expr::AggCall,
) -> StreamResult<AggCall> {
    let agg_kind = AggKind::try_from(agg_call_proto.get_type()?)?;
    let args = match &agg_call_proto.get_args()[..] {
        [] => AggArgs::None,
        [arg] if agg_kind != AggKind::StringAgg => AggArgs::Unary(
            DataType::from(arg.get_type()?),
            arg.get_input()?.column_idx as usize,
        ),
        [agg_arg, extra_arg] if agg_kind == AggKind::StringAgg => AggArgs::Binary(
            [
                DataType::from(agg_arg.get_type()?),
                DataType::from(extra_arg.get_type()?),
            ],
            [
                agg_arg.get_input()?.column_idx as usize,
                extra_arg.get_input()?.column_idx as usize,
            ],
        ),
        _ => bail!("Too many/few arguments for {:?}", agg_kind),
    };
    let mut order_pairs = vec![];
    let mut order_col_types = vec![];
    agg_call_proto
        .get_order_by_fields()
        .iter()
        .for_each(|field| {
            let col_idx = field.get_input().unwrap().get_column_idx() as usize;
            let col_type = DataType::from(field.get_type().unwrap());
            let order_type =
                OrderType::from_prost(&ProstOrderType::from_i32(field.direction).unwrap());
            // TODO(yuchao): `nulls first/last` is not supported yet, so it's ignore here,
            // see also `risingwave_common::util::sort_util::compare_values`
            order_pairs.push(OrderPair::new(col_idx, order_type));
            order_col_types.push(col_type);
        });
    let filter = match agg_call_proto.filter {
        Some(ref prost_filter) => Some(Arc::from(build_from_prost(prost_filter)?)),
        None => None,
    };
    Ok(AggCall {
        kind: agg_kind,
        args,
        return_type: DataType::from(agg_call_proto.get_return_type()?),
        order_pairs,
        append_only,
        filter,
    })
}
