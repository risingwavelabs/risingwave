use crate::array::*;
use crate::error::{ErrorCode, Result, RwError};
use crate::types::*;
use crate::util::{downcast_mut, downcast_ref};
use rust_decimal::Decimal;
use std::marker::PhantomData;

// Part 1: Public types and functions.

pub(crate) enum AggKind {
    Min,
    Max,
    Sum,
    Count,
}

pub(crate) trait AggState {
    fn update(&mut self, input: ArrayRef) -> Result<()>;
    fn output(&self, builder: &mut dyn ArrayBuilder) -> Result<()>;
}

pub(crate) fn create_agg_state(
    input_type: DataTypeRef,
    agg_type: &AggKind,
    return_type: DataTypeRef,
) -> Result<Box<dyn AggState>> {
    match (
        input_type.data_type_kind(),
        agg_type,
        return_type.data_type_kind(),
    ) {
        (_, AggKind::Count, DataTypeKind::Int64) => Ok(Count::new()),
        (DataTypeKind::Int16, AggKind::Sum, DataTypeKind::Int64) => {
            Ok(PrimitiveAgg::<Int16Type, _, Int64Type>::new(sum_i16))
        }
        (DataTypeKind::Int32, AggKind::Sum, DataTypeKind::Int64) => {
            Ok(PrimitiveAgg::<Int32Type, _, Int64Type>::new(sum_i32))
        }
        (DataTypeKind::Int64, AggKind::Sum, DataTypeKind::Decimal) => Ok(SumI64::new()),
        (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float32) => {
            Ok(PrimitiveAgg::<Float32Type, _, Float32Type>::new(sum_f32))
        }
        (DataTypeKind::Float64, AggKind::Sum, DataTypeKind::Float64) => {
            Ok(PrimitiveAgg::<Float64Type, _, Float64Type>::new(sum_f64))
        }
        (DataTypeKind::Decimal, AggKind::Sum, DataTypeKind::Decimal) => {
            Ok(DecimalAgg::new(sum_decimal))
        }
        (DataTypeKind::Int16, AggKind::Min, DataTypeKind::Int16) => {
            Ok(PrimitiveAgg::<Int16Type, _, Int16Type>::new(min_i16))
        }
        (DataTypeKind::Int32, AggKind::Min, DataTypeKind::Int32) => {
            Ok(PrimitiveAgg::<Int32Type, _, Int32Type>::new(min_i32))
        }
        (DataTypeKind::Int64, AggKind::Min, DataTypeKind::Int64) => {
            Ok(PrimitiveAgg::<Int64Type, _, Int64Type>::new(min_i64))
        }
        (DataTypeKind::Float32, AggKind::Min, DataTypeKind::Float32) => {
            Ok(PrimitiveAgg::<Float32Type, _, Float32Type>::new(min_f32))
        }
        (DataTypeKind::Float64, AggKind::Min, DataTypeKind::Float64) => {
            Ok(PrimitiveAgg::<Float64Type, _, Float64Type>::new(min_f64))
        }
        (DataTypeKind::Decimal, AggKind::Min, DataTypeKind::Decimal) => {
            Ok(DecimalAgg::new(min_decimal))
        }
        (DataTypeKind::Char, AggKind::Min, DataTypeKind::Char) => Ok(UTF8Agg::new(min_str)),
        _ => Err(RwError::from(ErrorCode::InternalError(String::from(
            "Unsupported agg.",
        )))),
    }
}

// Part 2: Private AggState implementations. Mostly follows/coupled with Array/ArrayBuilder
// implementations.

// PrimitiveAgg reads from PrimitiveArray and outputs to PrimitiveArrayBuilder.
struct PrimitiveAgg<T, F, R>
where
    T: PrimitiveDataType,
    F: FnMut(Option<R::N>, Option<T::N>) -> Option<R::N>,
    R: PrimitiveDataType,
{
    result: Option<R::N>,
    f: F,
    _phantom: PhantomData<T>,
}
impl<T, F, R> AggState for PrimitiveAgg<T, F, R>
where
    T: PrimitiveDataType,
    F: FnMut(Option<R::N>, Option<T::N>) -> Option<R::N>,
    R: PrimitiveDataType,
{
    fn update(&mut self, input: ArrayRef) -> Result<()> {
        let input: &PrimitiveArray<T> = downcast_ref(input.as_ref())?;
        self.result = input.as_iter()?.fold(self.result, &mut self.f);
        Ok(())
    }
    fn output(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
        let builder: &mut PrimitiveArrayBuilder<R> = downcast_mut(builder)?;
        builder.append_value(self.result)?;
        Ok(())
    }
}
impl<T, F, R> PrimitiveAgg<T, F, R>
where
    T: PrimitiveDataType,
    F: FnMut(Option<R::N>, Option<T::N>) -> Option<R::N>,
    R: PrimitiveDataType,
{
    fn new(f: F) -> Box<Self> {
        Box::new(Self {
            result: None,
            f,
            _phantom: PhantomData,
        })
    }
}

// DecimalAgg reads from DecimalArray and outputs to DecimalArrayBuilder.
struct DecimalAgg<F>
where
    F: FnMut(Option<Decimal>, Option<Decimal>) -> Option<Decimal>,
{
    result: Option<Decimal>,
    f: F,
}
impl<F> AggState for DecimalAgg<F>
where
    F: FnMut(Option<Decimal>, Option<Decimal>) -> Option<Decimal>,
{
    fn update(&mut self, input: ArrayRef) -> Result<()> {
        let input: &DecimalArray = downcast_ref(input.as_ref())?;
        self.result = input.as_iter()?.fold(self.result, &mut self.f);
        Ok(())
    }
    fn output(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
        let builder: &mut DecimalArrayBuilder = downcast_mut(builder)?;
        builder.append_value(self.result)?;
        Ok(())
    }
}
impl<F> DecimalAgg<F>
where
    F: FnMut(Option<Decimal>, Option<Decimal>) -> Option<Decimal>,
{
    fn new(f: F) -> Box<Self> {
        Box::new(Self { result: None, f })
    }
}

// SumI64 reads from PrimitiveArray and outputs to DecimalArrayBuilder. The only
// case right now is sum(i64).
struct SumI64 {
    result: Option<Decimal>,
}
impl AggState for SumI64 {
    fn update(&mut self, input: ArrayRef) -> Result<()> {
        let input: &PrimitiveArray<Int64Type> = downcast_ref(input.as_ref())?;
        self.result = input.as_iter()?.fold(self.result, sum_i64);
        Ok(())
    }
    fn output(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
        let builder: &mut DecimalArrayBuilder = downcast_mut(builder)?;
        builder.append_value(self.result)?;
        Ok(())
    }
}
impl SumI64 {
    fn new() -> Box<Self> {
        Box::new(Self { result: None })
    }
}

// UTF8Agg reads from UTF8Array and outputs to UTF8ArrayBuilder.
struct UTF8Agg<F>
where
    F: for<'a> FnMut(Option<&'a str>, Option<&'a str>) -> Option<&'a str>,
{
    result: Option<String>,
    f: F,
}
impl<F> AggState for UTF8Agg<F>
where
    F: for<'a> FnMut(Option<&'a str>, Option<&'a str>) -> Option<&'a str>,
{
    fn update(&mut self, input: ArrayRef) -> Result<()> {
        let input: &UTF8Array = downcast_ref(input.as_ref())?;
        let result = input
            .as_iter()?
            .fold(self.result.as_deref(), &mut self.f)
            .map(String::from);
        self.result = result;
        Ok(())
    }
    fn output(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
        let builder: &mut UTF8ArrayBuilder = downcast_mut(builder)?;
        builder.append_str(self.result.as_deref())?;
        Ok(())
    }
}
impl<F> UTF8Agg<F>
where
    F: for<'a> FnMut(Option<&'a str>, Option<&'a str>) -> Option<&'a str>,
{
    fn new(f: F) -> Box<Self> {
        Box::new(Self { result: None, f })
    }
}

// Count reads from Array and outputs to PrimitiveArrayBuilder.
struct Count {
    result: i64,
}
impl AggState for Count {
    fn update(&mut self, input: ArrayRef) -> Result<()> {
        self.result += input.len() as i64;
        Ok(())
    }
    fn output(&self, builder: &mut dyn ArrayBuilder) -> Result<()> {
        let builder: &mut PrimitiveArrayBuilder<Int64Type> = downcast_mut(builder)?;
        builder.append_value(Some(self.result))?;
        Ok(())
    }
}
impl Count {
    fn new() -> Box<Self> {
        Box::new(Self { result: 0 })
    }
}

// Part 3: Accumulator functions that gives new state after a single value.

macro_rules! sum {
    ($fn_name: ident, $input: ty, $result: ty) => {
        fn $fn_name(result: Option<$result>, input: Option<$input>) -> Option<$result> {
            match (result, input) {
                (_, None) => result,
                (None, Some(i)) => Some(<$result>::from(i)),
                (Some(r), Some(i)) => Some(r + <$result>::from(i)),
            }
        }
    };
}
sum!(sum_i16, i16, i64);
sum!(sum_i32, i32, i64);
sum!(sum_i64, i64, Decimal);
sum!(sum_f32, f32, f32);
sum!(sum_f64, f64, f64);
sum!(sum_decimal, Decimal, Decimal);

macro_rules! min {
    ($fn_name: ident, $input: ty) => {
        fn $fn_name(result: Option<$input>, input: Option<$input>) -> Option<$input> {
            match (result, input) {
                (_, None) => result,
                (None, Some(i)) => Some(i),
                (Some(r), Some(i)) => Some(if i < r { i } else { r }),
            }
        }
    };
}
min!(min_i16, i16);
min!(min_i32, i32);
min!(min_i64, i64);
min!(min_f32, f32);
min!(min_f64, f64);
min!(min_decimal, Decimal);

fn min_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Option<&'a str> {
    match (r, i) {
        (None, _) => i,
        (_, None) => r,
        (Some(rr), Some(ii)) => Some(if rr < ii { rr } else { ii }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn eval_agg(input: ArrayRef, agg_type: &AggKind, return_type: DataTypeRef) -> Result<ArrayRef> {
        let mut agg_state = create_agg_state(input.data_type_ref(), agg_type, return_type.clone())?;
        agg_state.update(input)?;
        let mut builder = return_type.create_array_builder(1)?;
        agg_state.output(builder.as_mut())?;
        builder.finish()
    }

    #[test]
    fn vec_sum_int32() -> Result<()> {
        let input = PrimitiveArray::<Int32Type>::from_slice(&[1, 2, 3])?;
        let agg_type = AggKind::Sum;
        let return_type = Arc::new(Int64Type::new(true));
        let actual = eval_agg(input, &agg_type, return_type)?;
        let actual: &PrimitiveArray<Int64Type> = downcast_ref(actual.as_ref())?;
        let actual = actual.as_slice();
        assert_eq!(actual, &[6]);
        Ok(())
    }

    #[test]
    fn vec_sum_int64() -> Result<()> {
        let input = PrimitiveArray::<Int64Type>::from_slice(&[1, 2, 3])?;
        let agg_type = AggKind::Sum;
        let return_type = Arc::new(DecimalType::new(true, 10, 0)?);
        let actual = eval_agg(input, &agg_type, return_type)?;
        let actual: &DecimalArray = downcast_ref(actual.as_ref())?;
        let actual = actual.as_iter()?.collect::<Vec<Option<Decimal>>>();
        assert_eq!(actual, vec![Some(Decimal::from(6))]);
        Ok(())
    }

    #[test]
    fn vec_min_float32() -> Result<()> {
        let input = PrimitiveArray::<Float32Type>::from_slice(&[1., 2., 3.])?;
        let agg_type = AggKind::Min;
        let return_type = Arc::new(Float32Type::new(true));
        let actual = eval_agg(input, &agg_type, return_type)?;
        let actual: &PrimitiveArray<Float32Type> = downcast_ref(actual.as_ref())?;
        let actual = actual.as_slice();
        assert_eq!(actual, &[1.]);
        Ok(())
    }

    #[test]
    fn vec_min_char() -> Result<()> {
        let input = UTF8Array::from_values(&[Some("b"), Some("aa")], 5, DataTypeKind::Char)?;
        let agg_type = AggKind::Min;
        let return_type = StringType::create(true, 5, DataTypeKind::Char);
        let actual = eval_agg(input, &agg_type, return_type)?;
        let actual: &UTF8Array = downcast_ref(actual.as_ref())?;
        let actual = actual.as_iter()?.collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("aa")]);
        Ok(())
    }

    #[test]
    fn vec_count_int32() -> Result<()> {
        let input = PrimitiveArray::<Int32Type>::from_slice(&[1, 2, 3])?;
        let agg_type = AggKind::Count;
        let return_type = Arc::new(Int64Type::new(true));
        let actual = eval_agg(input, &agg_type, return_type)?;
        let actual: &PrimitiveArray<Int64Type> = downcast_ref(actual.as_ref())?;
        let actual = actual.as_slice();
        assert_eq!(actual, &[3]);
        Ok(())
    }
}
