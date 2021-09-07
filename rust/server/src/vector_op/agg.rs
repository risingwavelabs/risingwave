use crate::array::{ArrayBuilder as ArrayBuilderV1, ArrayRef};
use crate::array2::*;
use crate::error::Result;
use crate::expr::AggKind;
use crate::types::*;
mod compare;
use compare::*;
mod count;
use count::*;
mod sum;
use sum::*;

/// `AggState` is the typed primitive for aggragation on array.
pub trait AggStateConcrete<I: Array> {
    /// `update` the aggragator with `Array` with input.
    fn update_concrete(&mut self, input: &I) -> Result<()>;
}

/// `AggFunction` pushes the result into an array builder.
pub trait AggFunctionConcrete<O: ArrayBuilder> {
    /// `output` the current result to an array builder.
    fn output_concrete(&self, builder: &mut O) -> Result<()>;
}

/// An `Aggregator` supports `update` data and `output` result.
pub trait Aggregator: Send + 'static {
    /// `update` the aggragator with `Array` with input with type checked at runtime.
    fn update(&mut self, input: &ArrayImpl) -> Result<()>;

    /// `output` the aggragator to `ArrayBuilder` with input with type checked at runtime.
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;
}

pub struct BoxedAggState(Box<dyn Aggregator>);

/// `BoxedAggState` is a wrapper for array v1 compactible layer.
/// TODO: remove this
impl BoxedAggState {
    pub fn update(&mut self, input: ArrayRef) -> Result<()> {
        self.0.update(&ArrayImpl::from(input))
    }
    pub fn output(&self, builder: &mut dyn ArrayBuilderV1) -> Result<()> {
        let mut my_builder = ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0));
        self.0.output(&mut my_builder)?;
        let array = my_builder.finish();
        let array = ArrayRef::from(array);
        builder.append_array(&*array)?;
        Ok(())
    }
}

pub fn create_agg_state_v2(
    input_type: DataTypeRef,
    agg_type: &AggKind,
    return_type: DataTypeRef,
) -> Result<Box<dyn Aggregator>> {
    let state: Box<dyn Aggregator> = match (
        input_type.data_type_kind(),
        agg_type,
        return_type.data_type_kind(),
    ) {
        (_, AggKind::Count, DataTypeKind::Int64) => Box::new(AggCount::new()),
        (DataTypeKind::Int16, AggKind::Sum, DataTypeKind::Int64) => {
            Box::new(AggSum::<I64Array, I16Array, PrimitiveSum<_, _>>::new())
        }
        (DataTypeKind::Int32, AggKind::Sum, DataTypeKind::Int64) => {
            Box::new(AggSum::<I64Array, I32Array, PrimitiveSum<_, _>>::new())
        }
        // (DataTypeKind::Int64, AggKind::Sum, DataTypeKind::Decimal) => Ok(SumI64Array::new()),
        (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float32) => {
            Box::new(AggSum::<F32Array, F32Array, PrimitiveSum<_, _>>::new())
        }
        (DataTypeKind::Float64, AggKind::Sum, DataTypeKind::Float64) => {
            Box::new(AggSum::<F64Array, F64Array, PrimitiveSum<_, _>>::new())
        }
        // (DataTypeKind::Decimal, AggKind::Sum, DataTypeKind::Decimal) => {
        //   Ok(DecimalAgg::new(sum_decimal))
        // }
        (DataTypeKind::Int16, AggKind::Min, DataTypeKind::Int16) => {
            Box::new(AggCompare::<I16Array, SimpleCompareMin<_>>::new())
        }
        (DataTypeKind::Int32, AggKind::Min, DataTypeKind::Int32) => {
            Box::new(AggCompare::<I32Array, SimpleCompareMin<_>>::new())
        }
        (DataTypeKind::Int64, AggKind::Min, DataTypeKind::Int64) => {
            Box::new(AggCompare::<I64Array, SimpleCompareMin<_>>::new())
        }
        (DataTypeKind::Float32, AggKind::Min, DataTypeKind::Float32) => {
            Box::new(AggCompare::<F32Array, SimpleCompareMin<_>>::new())
        }
        (DataTypeKind::Float64, AggKind::Min, DataTypeKind::Float64) => {
            Box::new(AggCompare::<F64Array, SimpleCompareMin<_>>::new())
        }
        // (DataTypeKind::Decimal, AggKind::Min, DataTypeKind::Decimal) => {
        //   Ok(DecimalAgg::new(min_decimal))
        // }
        (DataTypeKind::Char, AggKind::Min, DataTypeKind::Char) => {
            Box::new(AggCompare::<UTF8Array, SimpleCompareMin<_>>::new())
        }
        _ => unimplemented!(),
    };
    Ok(state)
}

pub fn create_agg_state(
    input_type: DataTypeRef,
    agg_type: &AggKind,
    return_type: DataTypeRef,
) -> Result<BoxedAggState> {
    Ok(BoxedAggState(create_agg_state_v2(
        input_type,
        agg_type,
        return_type,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn eval_agg(
        input_type: DataTypeRef,
        input: &ArrayImpl,
        agg_type: &AggKind,
        return_type: DataTypeRef,
        mut builder: ArrayBuilderImpl,
    ) -> Result<ArrayImpl> {
        let mut agg_state = create_agg_state_v2(input_type, agg_type, return_type.clone())?;
        agg_state.update(input)?;
        agg_state.output(&mut builder)?;
        Ok(builder.finish())
    }

    #[test]
    fn vec_sum_int32() -> Result<()> {
        let input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]);
        let agg_type = AggKind::Sum;
        let input_type = Arc::new(Int32Type::new(true));
        let return_type = Arc::new(Int64Type::new(true));
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)),
        )?;
        let actual = actual.as_int64();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(6)]);
        Ok(())
    }

    // #[test]
    // fn vec_sum_int64() -> Result<()> {
    //   let input = I64Array::from_slice(&[Some(1), Some(2), Some(3)])?;
    //   let agg_type = AggKind::Sum;
    //   let return_type = Arc::new(DecimalType::new(true, 10, 0)?);
    //   let actual = eval_agg(input, &agg_type, return_type)?;
    //   let actual: &DecimalArray = downcast_ref(actual.as_ref())?;
    //   let actual = actual.as_iter()?.collect::<Vec<Option<Decimal>>>();
    //   assert_eq!(actual, vec![Some(Decimal::from(6))]);
    //   Ok(())
    // }

    #[test]
    fn vec_min_float32() -> Result<()> {
        let input = F32Array::from_slice(&[Some(1.), Some(2.), Some(3.)]);
        let agg_type = AggKind::Min;
        let input_type = Arc::new(Float32Type::new(true));
        let return_type = Arc::new(Float32Type::new(true));
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Float32(F32ArrayBuilder::new(0)),
        )?;
        let actual = actual.as_float32();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(1.)]);
        Ok(())
    }

    #[test]
    fn vec_min_char() -> Result<()> {
        let input = UTF8Array::from_slice(&[Some("b"), Some("aa")]);
        let agg_type = AggKind::Min;
        let input_type = StringType::create(true, 5, DataTypeKind::Char);
        let return_type = StringType::create(true, 5, DataTypeKind::Char);
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::UTF8(UTF8ArrayBuilder::new(0)),
        )?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("aa")]);
        Ok(())
    }

    #[test]
    fn vec_count_int32() -> Result<()> {
        let input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]);
        let agg_type = AggKind::Count;
        let input_type = Arc::new(Int32Type::new(true));
        let return_type = Arc::new(Int64Type::new(true));
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)),
        )?;
        let actual = actual.as_int64();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(3)]);
        Ok(())
    }
}
