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

use dyn_clone::DynClone;
use risingwave_common::array::*;
use risingwave_common::bail;
use risingwave_common::types::*;

use crate::function::aggregate::{AggArgs, AggCall, AggKind};
use crate::Result;

mod approx_count_distinct;
mod array_agg;
mod count_star;
mod filter;
mod general;
mod general_sorted_grouper;
mod string_agg;

use self::approx_count_distinct::ApproxCountDistinct;
use self::array_agg::create_array_agg_state;
use self::count_star::CountStar;
use self::filter::*;
pub use self::general_sorted_grouper::{create_sorted_grouper, BoxedSortedGrouper, EqGroups};
use self::string_agg::create_string_agg_state;

/// An `Aggregator` supports `update` data and `output` result.
#[async_trait::async_trait]
pub trait Aggregator: Send + DynClone + 'static {
    fn return_type(&self) -> DataType;

    /// `update_single` update the aggregator with a single row with type checked at runtime.
    async fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        self.update_multi(input, row_id, row_id + 1).await
    }

    /// `update_multi` update the aggregator with multiple rows with type checked at runtime.
    async fn update_multi(
        &mut self,
        input: &DataChunk,
        start_row_id: usize,
        end_row_id: usize,
    ) -> Result<()>;

    /// `output` the aggregator to `ArrayBuilder` with input with type checked at runtime.
    /// After `output` the aggregator is reset to initial state.
    fn output(&mut self, builder: &mut ArrayBuilderImpl) -> Result<()>;
}

dyn_clone::clone_trait_object!(Aggregator);

pub type BoxedAggState = Box<dyn Aggregator>;

/// Build an `Aggregator` from `AggCall`.
pub fn build(agg_call: AggCall) -> Result<BoxedAggState> {
    // NOTE: The function signature is checked by `AggCall::infer_return_type` in the frontend.

    let initial_agg_state: BoxedAggState = match (agg_call.kind, agg_call.args) {
        (AggKind::Count, AggArgs::None) => Box::new(CountStar::new(agg_call.return_type.clone())),
        (AggKind::ApproxCountDistinct, AggArgs::Unary(_, arg_idx)) => Box::new(
            ApproxCountDistinct::new(agg_call.return_type.clone(), arg_idx),
        ),
        (AggKind::StringAgg, AggArgs::Binary([value_type, delim_type], [value_idx, delim_idx])) => {
            assert_eq!(value_type, DataType::Varchar);
            assert_eq!(delim_type, DataType::Varchar);
            create_string_agg_state(value_idx, delim_idx, agg_call.column_orders.clone())
        }
        (AggKind::ArrayAgg, AggArgs::Unary(_, arg_idx)) => create_array_agg_state(
            agg_call.return_type.clone(),
            arg_idx,
            agg_call.column_orders.clone(),
        ),
        (agg_kind, AggArgs::Unary(arg_type, arg_idx)) => {
            // other unary agg call
            create_agg_state_unary(
                arg_type,
                arg_idx,
                agg_kind,
                agg_call.return_type.clone(),
                agg_call.distinct,
            )?
        }
        (agg_kind, _) => bail!("Invalid agg call: {:?}", agg_kind),
    };

    // wrap the agg state in a `Filter` if needed
    let initial_agg_state = match agg_call.filter {
        Some(ref expr) => Box::new(Filter::new(expr.clone(), initial_agg_state)),
        None => initial_agg_state,
    };

    Ok(initial_agg_state)
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;

    use super::*;

    #[test]
    fn test_create_agg_state() {
        let int64_type = DataType::Int64;
        let decimal_type = DataType::Decimal;
        let bool_type = DataType::Boolean;
        let char_type = DataType::Varchar;
        macro_rules! test_create {
            ($input_type:expr, $agg:ident, $return_type:expr, $expected:ident) => {
                assert!(create_agg_state_unary(
                    $input_type.clone(),
                    0,
                    AggKind::$agg,
                    $return_type.clone(),
                    false,
                )
                .$expected());
                assert!(create_agg_state_unary(
                    $input_type.clone(),
                    0,
                    AggKind::$agg,
                    $return_type.clone(),
                    true,
                )
                .$expected());
            };
        }

        test_create! { int64_type, Count, int64_type, is_ok }
        test_create! { decimal_type, Count, int64_type, is_ok }
        test_create! { bool_type, Count, int64_type, is_ok }
        test_create! { char_type, Count, int64_type, is_ok }

        test_create! { int64_type, Sum, decimal_type, is_ok }
        test_create! { decimal_type, Sum, decimal_type, is_ok }
        test_create! { bool_type, Sum, bool_type, is_err }
        test_create! { char_type, Sum, char_type, is_err }

        test_create! { int64_type, Min, int64_type, is_ok }
        test_create! { decimal_type, Min, decimal_type, is_ok }
        test_create! { bool_type, Min, bool_type, is_ok } // TODO(#359): revert to is_err
        test_create! { char_type, Min, char_type, is_ok }
    }
}
