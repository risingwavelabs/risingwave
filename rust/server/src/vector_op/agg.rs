use crate::array2::*;
use crate::error::{ErrorCode, Result};
use crate::expr::AggKind;
use crate::types::*;
use std::marker::PhantomData;

/// An `Aggregator` supports `update` data and `output` result.
pub trait Aggregator: Send + 'static {
    /// `update` the aggragator with `Array` with input with type checked at runtime.
    fn update(&mut self, input: &ArrayImpl) -> Result<()>;

    /// `output` the aggragator to `ArrayBuilder` with input with type checked at runtime.
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;
}

pub type BoxedAggState = Box<dyn Aggregator>;

pub fn create_agg_state(
    input_type: DataTypeRef,
    agg_type: &AggKind,
    return_type: DataTypeRef,
) -> Result<Box<dyn Aggregator>> {
    let state: Box<dyn Aggregator> = match (
        input_type.data_type_kind(),
        agg_type,
        return_type.data_type_kind(),
    ) {
        // TODO(xiangjin): Ideally count non-null on Array without checking its type.
        (DataTypeKind::Int16, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<I16Array, _, _>::new(count))
        }
        (DataTypeKind::Int32, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<I32Array, _, _>::new(count))
        }
        (DataTypeKind::Int64, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<I64Array, _, _>::new(count))
        }
        (DataTypeKind::Float32, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<F32Array, _, _>::new(count))
        }
        (DataTypeKind::Float64, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<F64Array, _, _>::new(count))
        }
        (DataTypeKind::Char, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<UTF8Array, _, _>::new(count_str))
        }
        (DataTypeKind::Boolean, AggKind::Count, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<BoolArray, _, _>::new(count))
        }
        (DataTypeKind::Int16, AggKind::Sum, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<I16Array, _, I64Array>::new(sum))
        }
        (DataTypeKind::Int32, AggKind::Sum, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<I32Array, _, I64Array>::new(sum))
        }
        // (DataTypeKind::Int64, AggKind::Sum, DataTypeKind::Decimal) => todo!(),
        (DataTypeKind::Float32, AggKind::Sum, DataTypeKind::Float32) => {
            Box::new(GeneralAgg::<F32Array, _, F32Array>::new(sum))
        }
        (DataTypeKind::Float64, AggKind::Sum, DataTypeKind::Float64) => {
            Box::new(GeneralAgg::<F64Array, _, F64Array>::new(sum))
        }
        // (DataTypeKind::Decimal, AggKind::Sum, DataTypeKind::Decimal) => {
        //   todo!()
        // }
        (DataTypeKind::Int16, AggKind::Min, DataTypeKind::Int16) => {
            Box::new(GeneralAgg::<I16Array, _, I16Array>::new(min))
        }
        (DataTypeKind::Int32, AggKind::Min, DataTypeKind::Int32) => {
            Box::new(GeneralAgg::<I32Array, _, I32Array>::new(min))
        }
        (DataTypeKind::Int64, AggKind::Min, DataTypeKind::Int64) => {
            Box::new(GeneralAgg::<I64Array, _, I64Array>::new(min))
        }
        (DataTypeKind::Float32, AggKind::Min, DataTypeKind::Float32) => {
            Box::new(GeneralAgg::<F32Array, _, F32Array>::new(min))
        }
        (DataTypeKind::Float64, AggKind::Min, DataTypeKind::Float64) => {
            Box::new(GeneralAgg::<F64Array, _, F64Array>::new(min))
        }
        // (DataTypeKind::Decimal, AggKind::Min, DataTypeKind::Decimal) => {
        //   todo!()
        // }
        (DataTypeKind::Char, AggKind::Min, DataTypeKind::Char) => {
            Box::new(GeneralAgg::<UTF8Array, _, UTF8Array>::new(min_str))
        }
        (unimpl_input, unimpl_agg, unimpl_ret) => todo!(
            "unsupported aggregator: type={:?} input={:?} output={:?}",
            unimpl_agg,
            unimpl_input,
            unimpl_ret
        ),
    };
    Ok(state)
}

struct GeneralAgg<T, F, R>
where
    T: Array,
    F: Send + for<'a> RTFn<'a, T, R>,
    R: Array,
{
    result: Option<R::OwnedItem>,
    f: F,
    _phantom: PhantomData<T>,
}
impl<T, F, R> GeneralAgg<T, F, R>
where
    T: Array,
    F: Send + for<'a> RTFn<'a, T, R>,
    R: Array,
{
    fn new(f: F) -> Self {
        Self {
            result: None,
            f,
            _phantom: PhantomData,
        }
    }
    fn update_concrete(&mut self, input: &T) -> Result<()> {
        let r = input
            .iter()
            .fold(self.result.as_ref().map(|x| x.as_scalar_ref()), &mut self.f)
            .map(|x| x.to_owned_scalar());
        self.result = r;
        Ok(())
    }
    fn output_concrete(&self, builder: &mut R::Builder) -> Result<()> {
        builder.append(self.result.as_ref().map(|x| x.as_scalar_ref()))
    }
}
/// Essentially `RTFn` is an alias of the specific Fn. It was aliased not to
/// shorten the `where` clause of `GeneralAgg`, but to workaround an compiler
/// error[E0582]: binding for associated type `Output` references lifetime `'a`,
/// which does not appear in the trait input types.
trait RTFn<'a, T, R>:
    Fn(Option<R::RefItem<'a>>, Option<T::RefItem<'a>>) -> Option<R::RefItem<'a>>
where
    T: Array,
    R: Array,
{
}

impl<'a, T, R, Z> RTFn<'a, T, R> for Z
where
    T: Array,
    R: Array,
    Z: Fn(Option<R::RefItem<'a>>, Option<T::RefItem<'a>>) -> Option<R::RefItem<'a>>,
{
}

macro_rules! impl_aggregator {
    ($input:ty, $input_variant:ident, $result:ty, $result_variant:ident) => {
        impl<F> Aggregator for GeneralAgg<$input, F, $result>
        where
            F: 'static + Send + for<'a> RTFn<'a, $input, $result>,
        {
            fn update(&mut self, input: &ArrayImpl) -> Result<()> {
                if let ArrayImpl::$input_variant(i) = input {
                    self.update_concrete(i)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Input fail to match {}.",
                        stringify!($input_variant)
                    ))
                    .into())
                }
            }
            fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                if let ArrayBuilderImpl::$result_variant(b) = builder {
                    self.output_concrete(b)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Builder fail to match {}.",
                        stringify!($result_variant)
                    ))
                    .into())
                }
            }
        }
    };
}
impl_aggregator! { I16Array, Int16, I16Array, Int16 }
impl_aggregator! { I32Array, Int32, I32Array, Int32 }
impl_aggregator! { I64Array, Int64, I64Array, Int64 }
impl_aggregator! { F32Array, Float32, F32Array, Float32 }
impl_aggregator! { F64Array, Float64, F64Array, Float64 }
impl_aggregator! { UTF8Array, UTF8, UTF8Array, UTF8 }
impl_aggregator! { I16Array, Int16, I64Array, Int64 }
impl_aggregator! { I32Array, Int32, I64Array, Int64 }
impl_aggregator! { F32Array, Float32, I64Array, Int64 }
impl_aggregator! { F64Array, Float64, I64Array, Int64 }
impl_aggregator! { UTF8Array, UTF8, I64Array, Int64 }
impl_aggregator! { BoolArray, Bool, I64Array, Int64 }

use std::convert::From;
use std::ops::Add;

fn sum<R, T>(result: Option<R>, input: Option<T>) -> Option<R>
where
    R: From<T> + Add<Output = R> + Copy,
{
    match (result, input) {
        (_, None) => result,
        (None, Some(i)) => Some(R::from(i)),
        (Some(r), Some(i)) => Some(r + R::from(i)),
    }
}

use std::cmp::PartialOrd;

fn min<'a, T>(result: Option<T>, input: Option<T>) -> Option<T>
where
    T: ScalarRef<'a> + PartialOrd,
{
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(if r < i { r } else { i }),
    }
}

fn min_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Option<&'a str> {
    min(r, i)
}

fn count<T>(result: Option<i64>, input: Option<T>) -> Option<i64> {
    match (result, input) {
        (_, None) => result,
        (None, Some(_)) => Some(1),
        (Some(r), Some(_)) => Some(r + 1),
    }
}

fn count_str(r: Option<i64>, i: Option<&str>) -> Option<i64> {
    count(r, i)
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
        let mut agg_state = create_agg_state(input_type, agg_type, return_type.clone())?;
        agg_state.update(input)?;
        agg_state.output(&mut builder)?;
        builder.finish()
    }

    #[test]
    fn vec_sum_int32() -> Result<()> {
        let input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        let agg_type = AggKind::Sum;
        let input_type = Arc::new(Int32Type::new(true));
        let return_type = Arc::new(Int64Type::new(true));
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)?),
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
        let input = F32Array::from_slice(&[Some(1.), Some(2.), Some(3.)]).unwrap();
        let agg_type = AggKind::Min;
        let input_type = Arc::new(Float32Type::new(true));
        let return_type = Arc::new(Float32Type::new(true));
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Float32(F32ArrayBuilder::new(0)?),
        )?;
        let actual = actual.as_float32();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(1.)]);
        Ok(())
    }

    #[test]
    fn vec_min_char() -> Result<()> {
        let input = UTF8Array::from_slice(&[Some("b"), Some("aa")])?;
        let agg_type = AggKind::Min;
        let input_type = StringType::create(true, 5, DataTypeKind::Char);
        let return_type = StringType::create(true, 5, DataTypeKind::Char);
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::UTF8(UTF8ArrayBuilder::new(0)?),
        )?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("aa")]);
        Ok(())
    }

    #[test]
    fn vec_count_int32() -> Result<()> {
        let input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        let agg_type = AggKind::Count;
        let input_type = Arc::new(Int32Type::new(true));
        let return_type = Arc::new(Int64Type::new(true));
        let actual = eval_agg(
            input_type,
            &input.into(),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)?),
        )?;
        let actual = actual.as_int64();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(3)]);
        Ok(())
    }
}
