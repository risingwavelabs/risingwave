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

use std::sync::Arc;

use dyn_clone::DynClone;
use risingwave_common::array::*;
use risingwave_common::bail;
use risingwave_common::types::*;
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_pb::expr::AggCall;
use risingwave_pb::plan_common::OrderType as ProstOrderType;

use super::array_agg::create_array_agg_state;
use crate::expr::{build_from_prost, AggKind, Expression, ExpressionRef, LiteralExpression};
use crate::vector_op::agg::approx_count_distinct::ApproxCountDistinct;
use crate::vector_op::agg::count_star::CountStar;
use crate::vector_op::agg::functions::*;
use crate::vector_op::agg::general_agg::*;
use crate::vector_op::agg::general_distinct_agg::*;
use crate::vector_op::agg::string_agg::create_string_agg_state;
use crate::Result;

/// An `Aggregator` supports `update` data and `output` result.
pub trait Aggregator: Send + DynClone + 'static {
    fn return_type(&self) -> DataType;

    /// `update_single` update the aggregator with a single row with type checked at runtime.
    fn update_single(&mut self, input: &DataChunk, row_id: usize) -> Result<()>;

    /// `update_multi` update the aggregator with multiple rows with type checked at runtime.
    fn update_multi(
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

pub struct AggStateFactory {
    /// Return type of the agg call.
    return_type: DataType,
    /// The _prototype_ of agg state. It is cloned when need to create a new agg state.
    initial_agg_state: BoxedAggState,
}

impl AggStateFactory {
    pub fn new(prost: &AggCall) -> Result<Self> {
        // NOTE: The function signature is checked by `AggCall::infer_return_type` in the frontend.

        let return_type = DataType::from(prost.get_return_type()?);
        let agg_kind = AggKind::try_from(prost.get_type()?)?;
        let distinct = prost.distinct;
        let mut order_pairs = vec![];
        let mut order_col_types = vec![];
        prost.get_order_by_fields().iter().for_each(|field| {
            let col_idx = field.get_input().unwrap().get_column_idx() as usize;
            let col_type = DataType::from(field.get_type().unwrap());
            let order_type =
                OrderType::from_prost(&ProstOrderType::from_i32(field.direction).unwrap());
            // TODO(yuchao): `nulls first/last` is not supported yet, so it's ignore here,
            // see also `risingwave_common::util::sort_util::compare_values`
            order_pairs.push(OrderPair::new(col_idx, order_type));
            order_col_types.push(col_type);
        });
        let filter: ExpressionRef = match prost.filter {
            Some(ref expr) => Arc::from(build_from_prost(expr)?),
            None => Arc::from(
                LiteralExpression::new(DataType::Boolean, Some(ScalarImpl::Bool(true))).boxed(),
            ),
        };

        let initial_agg_state: BoxedAggState = match (agg_kind, &prost.get_args()[..]) {
            (AggKind::Count, []) => Box::new(CountStar::new(return_type.clone(), filter)),
            (AggKind::ApproxCountDistinct, [arg]) => {
                let input_col_idx = arg.get_input()?.get_column_idx() as usize;
                Box::new(ApproxCountDistinct::new(
                    return_type.clone(),
                    input_col_idx,
                    filter,
                ))
            }
            (AggKind::StringAgg, [agg_arg, delim_arg]) => {
                assert_eq!(
                    DataType::from(agg_arg.get_type().unwrap()),
                    DataType::Varchar
                );
                assert_eq!(
                    DataType::from(delim_arg.get_type().unwrap()),
                    DataType::Varchar
                );
                let agg_col_idx = agg_arg.get_input()?.get_column_idx() as usize;
                let delim_col_idx = delim_arg.get_input()?.get_column_idx() as usize;
                create_string_agg_state(agg_col_idx, delim_col_idx, order_pairs)?
            }
            (AggKind::ArrayAgg, [arg]) => {
                let agg_col_idx = arg.get_input()?.get_column_idx() as usize;
                create_array_agg_state(return_type.clone(), agg_col_idx, order_pairs)?
            }
            (agg_kind, [arg]) => {
                // other unary agg call
                let input_type = DataType::from(arg.get_type()?);
                let input_col_idx = arg.get_input()?.get_column_idx() as usize;
                create_agg_state_unary(
                    input_type,
                    input_col_idx,
                    agg_kind,
                    return_type.clone(),
                    distinct,
                    filter,
                )?
            }
            _ => bail!("Invalid agg call: {:?}", agg_kind),
        };

        Ok(Self {
            return_type,
            initial_agg_state,
        })
    }

    pub fn create_agg_state(&self) -> BoxedAggState {
        self.initial_agg_state.clone()
    }

    pub fn get_return_type(&self) -> DataType {
        self.return_type.clone()
    }
}

pub fn create_agg_state_unary(
    input_type: DataType,
    input_col_idx: usize,
    agg_kind: AggKind,
    return_type: DataType,
    distinct: bool,
    filter: ExpressionRef,
) -> Result<BoxedAggState> {
    use crate::expr::data_types::*;

    macro_rules! gen_arms {
        [$(($agg:ident, $fn:expr, $in:tt, $ret:tt, $init_result:expr)),* $(,)?] => {
            match (
                input_type,
                agg_kind,
                return_type.clone(),
                distinct,
            ) {
                $(
                    ($in! { type_match_pattern }, AggKind::$agg, $ret! { type_match_pattern }, false) => {
                        Box::new(GeneralAgg::<$in! { type_array }, _, $ret! { type_array }>::new(
                            return_type,
                            input_col_idx,
                            $fn,
                            $init_result,
                            filter
                        ))
                    },
                    ($in! { type_match_pattern }, AggKind::$agg, $ret! { type_match_pattern }, true) => {
                        Box::new(GeneralDistinctAgg::<$in! { type_array }, _, $ret! { type_array }>::new(
                            return_type,
                            input_col_idx,
                            $fn,
                            filter,
                        ))
                    },
                )*
                (unimpl_input, unimpl_agg, unimpl_ret, distinct) => {
                    bail!(
                        "unsupported aggregator: type={:?} input={:?} output={:?} distinct={}",
                        unimpl_agg, unimpl_input, unimpl_ret, distinct
                    )
                }
            }
        };
    }

    let state: BoxedAggState = gen_arms![
        (Count, count, int16, int64, Some(0)),
        (Count, count, int32, int64, Some(0)),
        (Count, count, int64, int64, Some(0)),
        (Count, count, float32, int64, Some(0)),
        (Count, count, float64, int64, Some(0)),
        (Count, count, decimal, int64, Some(0)),
        (Count, count_str, varchar, int64, Some(0)),
        (Count, count, boolean, int64, Some(0)),
        (Count, count, interval, int64, Some(0)),
        (Count, count, date, int64, Some(0)),
        (Count, count, timestamp, int64, Some(0)),
        (Count, count, time, int64, Some(0)),
        (Count, count_struct, struct_type, int64, Some(0)),
        (Count, count_list, list, int64, Some(0)),
        (Sum, sum, int16, int64, None),
        (Sum, sum, int32, int64, None),
        (Sum, sum, int64, decimal, None),
        (Sum, sum, float32, float32, None),
        (Sum, sum, float64, float64, None),
        (Sum, sum, decimal, decimal, None),
        (Min, min, int16, int16, None),
        (Min, min, int32, int32, None),
        (Min, min, int64, int64, None),
        (Min, min, float32, float32, None),
        (Min, min, float64, float64, None),
        (Min, min, decimal, decimal, None),
        (Min, min, boolean, boolean, None), // TODO(#359): remove once unnecessary
        (Min, min, interval, interval, None),
        (Min, min, date, date, None),
        (Min, min, timestamp, timestamp, None),
        (Min, min, time, time, None),
        (Min, min_struct, struct_type, struct_type, None),
        (Min, min_str, varchar, varchar, None),
        (Min, min_list, list, list, None),
        (Max, max, int16, int16, None),
        (Max, max, int32, int32, None),
        (Max, max, int64, int64, None),
        (Max, max, float32, float32, None),
        (Max, max, float64, float64, None),
        (Max, max, decimal, decimal, None),
        (Max, max, boolean, boolean, None), // TODO(#359): remove once unnecessary
        (Max, max, interval, interval, None),
        (Max, max, date, date, None),
        (Max, max, timestamp, timestamp, None),
        (Max, max, time, time, None),
        (Max, max_struct, struct_type, struct_type, None),
        (Max, max_str, varchar, varchar, None),
        (Max, max_list, list, list, None),
        // Global Agg
        (Sum, sum, int64, int64, None),
        // We remark that SingleValue does not produce a runtime error when it receives zero row.
        // Therefore, we do NOT need to change the logic in GeneralAgg::output_concrete.
        (SingleValue, SingleValue::new(), int16, int16, None),
        (SingleValue, SingleValue::new(), int32, int32, None),
        (SingleValue, SingleValue::new(), int64, int64, None),
        (SingleValue, SingleValue::new(), float32, float32, None),
        (SingleValue, SingleValue::new(), float64, float64, None),
        (SingleValue, SingleValue::new(), decimal, decimal, None),
        (SingleValue, SingleValue::new(), boolean, boolean, None),
        (SingleValue, SingleValue::new(), varchar, varchar, None),
    ];
    Ok(state)
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
        let filter: ExpressionRef = Arc::from(
            LiteralExpression::new(DataType::Boolean, Some(ScalarImpl::Bool(true))).boxed(),
        );
        macro_rules! test_create {
            ($input_type:expr, $agg:ident, $return_type:expr, $expected:ident) => {
                assert!(create_agg_state_unary(
                    $input_type.clone(),
                    0,
                    AggKind::$agg,
                    $return_type.clone(),
                    false,
                    filter.clone(),
                )
                .$expected());
                assert!(create_agg_state_unary(
                    $input_type.clone(),
                    0,
                    AggKind::$agg,
                    $return_type.clone(),
                    true,
                    filter.clone(),
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

        test_create! { int64_type, SingleValue, int64_type, is_ok }
        test_create! { decimal_type, SingleValue, decimal_type, is_ok }
        test_create! { bool_type, SingleValue, bool_type, is_ok }
        test_create! { char_type, SingleValue, char_type, is_ok }
    }
}
