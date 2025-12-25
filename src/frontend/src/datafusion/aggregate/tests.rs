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

use std::ops::Range;
use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType as DFDataType, Field as DFField};
use datafusion::logical_expr::function::StateFieldsArgs;
use datafusion::logical_expr::{AggregateUDFImpl, Signature, TypeSignature, Volatility};
use risingwave_common::array::StreamChunk;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, ScalarImpl, ScalarRefImpl};
use risingwave_expr::Result as RwResult;
use risingwave_expr::aggregate::{AggregateFunction, AggregateState, BoxedAggregateFunction};

use super::single_phase::AggregateFunction as SinglePhaseAgg;
use super::two_phase::AggregateFunction as TwoPhaseAgg;
use crate::datafusion::CastExecutor;

/// Mock aggregate function that sums Int64 values - used for testing
#[derive(Debug)]
pub struct MockSumAgg;

#[async_trait::async_trait]
impl AggregateFunction for MockSumAgg {
    fn return_type(&self) -> DataType {
        DataType::Int64
    }

    fn create_state(&self) -> RwResult<AggregateState> {
        Ok(AggregateState::Datum(Some(ScalarImpl::Int64(0))))
    }

    async fn update(&self, state: &mut AggregateState, input: &StreamChunk) -> RwResult<()> {
        let current = match state {
            AggregateState::Datum(Some(ScalarImpl::Int64(n))) => *n,
            _ => 0,
        };

        let mut sum = current;
        for (_, row) in input.rows() {
            if let Some(ScalarRefImpl::Int64(val)) = row.datum_at(0) {
                sum += val;
            }
        }
        *state = AggregateState::Datum(Some(ScalarImpl::Int64(sum)));
        Ok(())
    }

    async fn update_range(
        &self,
        state: &mut AggregateState,
        input: &StreamChunk,
        range: Range<usize>,
    ) -> RwResult<()> {
        let current = match state {
            AggregateState::Datum(Some(ScalarImpl::Int64(n))) => *n,
            _ => 0,
        };

        let mut sum = current;
        for (_, row) in input.rows_in(range) {
            if let Some(ScalarRefImpl::Int64(val)) = row.datum_at(0) {
                sum += val;
            }
        }
        *state = AggregateState::Datum(Some(ScalarImpl::Int64(sum)));
        Ok(())
    }

    async fn get_result(&self, state: &AggregateState) -> RwResult<Option<ScalarImpl>> {
        match state {
            AggregateState::Datum(Some(value)) => Ok(Some(value.clone())),
            _ => Ok(None),
        }
    }
}

#[test]
fn test_single_phase_properties() {
    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();

    let func: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };

    let agg = SinglePhaseAgg::new(
        "sum".to_owned(),
        vec![DataType::Int64],
        cast,
        func,
        signature,
    );

    assert_eq!(agg.name(), "sum");
    assert_eq!(
        agg.return_type(&[DFDataType::Int64]).unwrap(),
        DFDataType::Int64
    );

    let input_fields = vec![Arc::new(DFField::new("input", DFDataType::Int64, true))];
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = StateFieldsArgs {
        name: "agg",
        input_fields: &input_fields,
        ordering_fields: &[],
        is_distinct: false,
        return_field,
    };

    let fields = agg.state_fields(args).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "agg[0]");
    assert_eq!(fields[0].data_type(), &DFDataType::Binary);
}

#[test]
fn test_two_phase_properties() {
    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();

    let phase1: BoxedAggregateFunction = Box::new(MockSumAgg);
    let phase2: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };

    let agg = TwoPhaseAgg::new("sum".to_owned(), cast, phase1, phase2, signature).unwrap();

    assert_eq!(agg.name(), "sum");
    assert_eq!(
        agg.return_type(&[DFDataType::Int64]).unwrap(),
        DFDataType::Int64
    );

    let input_fields = vec![Arc::new(DFField::new("input", DFDataType::Int64, true))];
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = StateFieldsArgs {
        name: "agg",
        input_fields: &input_fields,
        ordering_fields: &[],
        is_distinct: false,
        return_field,
    };

    let fields = agg.state_fields(args).unwrap();
    assert_eq!(fields.len(), 1);
    assert_eq!(fields[0].name(), "agg_state");
    assert_eq!(fields[0].data_type(), &DFDataType::Int64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_single_phase_accumulator() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::logical_expr::AggregateUDFImpl;
    use datafusion::logical_expr::function::AccumulatorArgs;

    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();

    let func: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };

    let agg = SinglePhaseAgg::new(
        "sum".to_owned(),
        vec![DataType::Int64],
        cast,
        func,
        signature,
    );

    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        DFField::new("value", DFDataType::Int64, true),
    ]));
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = AccumulatorArgs {
        schema: &schema,
        exprs: &[],
        is_distinct: false,
        name: "sum",
        ignore_nulls: false,
        is_reversed: false,
        return_field,
        order_bys: &[],
    };

    let mut acc = agg.accumulator(args).unwrap();

    // Test update_batch with DataFusion Arrow arrays
    let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 30]));
    acc.update_batch(&[array]).unwrap();

    // Test size
    assert!(acc.size() > 0);

    // Test evaluate
    let result = acc.evaluate().unwrap();
    if let datafusion_common::ScalarValue::Int64(Some(val)) = result {
        assert_eq!(val, 60);
    } else {
        panic!("Expected Int64(Some(60))");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_single_phase_accumulator_state_and_merge() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::logical_expr::AggregateUDFImpl;
    use datafusion::logical_expr::function::AccumulatorArgs;

    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();

    let func: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };

    let agg = SinglePhaseAgg::new(
        "sum".to_owned(),
        vec![DataType::Int64],
        cast,
        func,
        signature,
    );

    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        DFField::new("value", DFDataType::Int64, true),
    ]));
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = AccumulatorArgs {
        schema: &schema,
        exprs: &[],
        is_distinct: false,
        name: "sum",
        ignore_nulls: false,
        is_reversed: false,
        return_field,
        order_bys: &[],
    };

    let mut acc1 = agg.accumulator(args.clone()).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![10, 20]));
    acc1.update_batch(&[array]).unwrap();

    // Test state extraction
    let state = acc1.state().unwrap();
    assert_eq!(state.len(), 1);
    assert!(matches!(
        state[0],
        datafusion_common::ScalarValue::Binary(_)
    ));

    // Test merge into second accumulator
    let mut acc2 = agg.accumulator(args).unwrap();
    let state_arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array()).try_collect().unwrap();
    acc2.merge_batch(&state_arrays).unwrap();
    let result = acc2.evaluate().unwrap();
    assert!(matches!(result, datafusion_common::ScalarValue::Int64(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_single_phase_accumulator_empty_batch() {
    use datafusion::logical_expr::AggregateUDFImpl;
    use datafusion::logical_expr::function::AccumulatorArgs;

    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();

    let func: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };

    let agg = SinglePhaseAgg::new(
        "sum".to_owned(),
        vec![DataType::Int64],
        cast,
        func,
        signature,
    );

    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        DFField::new("value", DFDataType::Int64, true),
    ]));
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = AccumulatorArgs {
        schema: &schema,
        exprs: &[],
        is_distinct: false,
        name: "sum",
        ignore_nulls: false,
        is_reversed: false,
        return_field,
        order_bys: &[],
    };

    let mut acc = agg.accumulator(args).unwrap();

    // Should still be able to evaluate
    let result = acc.evaluate().unwrap();
    assert!(matches!(result, datafusion_common::ScalarValue::Int64(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_two_phase_accumulator() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::logical_expr::AggregateUDFImpl;
    use datafusion::logical_expr::function::AccumulatorArgs;

    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();

    let phase1: BoxedAggregateFunction = Box::new(MockSumAgg);
    let phase2: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };

    let agg = TwoPhaseAgg::new("sum".to_owned(), cast, phase1, phase2, signature).unwrap();

    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        DFField::new("value", DFDataType::Int64, true),
    ]));
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = AccumulatorArgs {
        schema: &schema,
        exprs: &[],
        is_distinct: false,
        name: "sum",
        ignore_nulls: false,
        is_reversed: false,
        return_field,
        order_bys: &[],
    };

    let mut acc = agg.accumulator(args).unwrap();

    // Test update_batch
    let array: ArrayRef = Arc::new(Int64Array::from(vec![5, 10, 15]));
    acc.update_batch(&[array]).unwrap();

    // Test size
    assert!(acc.size() > 0);

    // Test evaluate
    let result = acc.evaluate().unwrap();
    if let datafusion_common::ScalarValue::Int64(Some(val)) = result {
        assert_eq!(val, 30);
    } else {
        panic!("Expected Int64(Some(30))");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_two_phase_accumulator_state_and_merge() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::logical_expr::AggregateUDFImpl;
    use datafusion::logical_expr::function::AccumulatorArgs;

    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();

    let phase1: BoxedAggregateFunction = Box::new(MockSumAgg);
    let phase2: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };

    let agg = TwoPhaseAgg::new("sum".to_owned(), cast, phase1, phase2, signature).unwrap();

    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        DFField::new("value", DFDataType::Int64, true),
    ]));
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = AccumulatorArgs {
        schema: &schema,
        exprs: &[],
        is_distinct: false,
        name: "sum",
        ignore_nulls: false,
        is_reversed: false,
        return_field,
        order_bys: &[],
    };

    let mut acc1 = agg.accumulator(args.clone()).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![5, 15]));
    acc1.update_batch(&[array]).unwrap();

    // Test state extraction
    let state = acc1.state().unwrap();
    assert_eq!(state.len(), 1);
    assert!(matches!(state[0], datafusion_common::ScalarValue::Int64(_),));

    // Test merge into second accumulator
    let mut acc2 = agg.accumulator(args).unwrap();
    let state_arrays: Vec<ArrayRef> = state.iter().map(|s| s.to_array()).try_collect().unwrap();
    acc2.merge_batch(&state_arrays).unwrap();
    let result = acc2.evaluate().unwrap();
    assert!(matches!(result, datafusion_common::ScalarValue::Int64(_)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_two_phase_accumulator_empty_batch() {
    use datafusion::logical_expr::AggregateUDFImpl;
    use datafusion::logical_expr::function::AccumulatorArgs;

    let cast = CastExecutor::from_iter(
        std::iter::once(DFDataType::Int64),
        std::iter::once(DataType::Int64),
    )
    .unwrap();
    let phase1: BoxedAggregateFunction = Box::new(MockSumAgg);
    let phase2: BoxedAggregateFunction = Box::new(MockSumAgg);
    let signature = Signature {
        type_signature: TypeSignature::Any(1),
        volatility: Volatility::Volatile,
    };
    let agg = TwoPhaseAgg::new("sum".to_owned(), cast, phase1, phase2, signature).unwrap();
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        DFField::new("value", DFDataType::Int64, true),
    ]));
    let return_field = Arc::new(DFField::new("result", DFDataType::Int64, false));
    let args = AccumulatorArgs {
        schema: &schema,
        exprs: &[],
        is_distinct: false,
        name: "sum",
        ignore_nulls: false,
        is_reversed: false,
        return_field,
        order_bys: &[],
    };
    let mut acc = agg.accumulator(args).unwrap();

    // Should still be able to evaluate
    let result = acc.evaluate().unwrap();
    assert!(matches!(result, datafusion_common::ScalarValue::Int64(_)));
}
