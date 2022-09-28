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
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_expr::expr::{build_from_prost, AggKind};
use risingwave_pb::plan_common::OrderType as ProstOrderType;
use risingwave_storage::table::streaming_table::state_table::StateTable;

use super::*;
use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::{AggArgs, AggCall, AggStateTable};

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

/// Parse from stream proto plan agg call states, generate state tables and column mappings.
/// The `vnodes` is generally `Some` for Hash Agg and `None` for Simple Agg.
pub fn build_agg_state_tables_from_proto<S: StateStore>(
    store: S,
    agg_call_states: &[risingwave_pb::stream_plan::AggCallState],
    vnodes: Option<Arc<Bitmap>>,
) -> Vec<Option<AggStateTable<S>>> {
    use risingwave_pb::stream_plan::agg_call_state;

    agg_call_states
        .iter()
        .map(|state| match state.get_inner().unwrap() {
            agg_call_state::Inner::ResultValueState(..) => None,
            agg_call_state::Inner::MaterializedState(state) => {
                let table =
                    StateTable::from_table_catalog(state.get_table().unwrap(), store.clone(), None);
                let mapping = StateTableColumnMapping::new(
                    state
                        .get_upstream_column_indices()
                        .iter()
                        .map(|idx| *idx as usize)
                        .collect(),
                );
                Some(AggStateTable { table, mapping })
            }
        })
        .collect()
}
