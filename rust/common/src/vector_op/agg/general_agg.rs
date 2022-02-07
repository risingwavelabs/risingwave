use std::cmp::PartialOrd;
use std::marker::PhantomData;

use crate::array::*;
use crate::error::{ErrorCode, Result};
use crate::types::*;
use crate::vector_op::agg::aggregator::Aggregator;
use crate::vector_op::agg::general_sorted_grouper::EqGroups;

pub struct GeneralAgg<T, F, R>
where
    T: Array,
    F: Send + for<'a> RTFn<'a, T, R>,
    R: Array,
{
    return_type: DataType,
    input_col_idx: usize,
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
    pub fn new(return_type: DataType, input_col_idx: usize, f: F) -> Self {
        Self {
            return_type,
            input_col_idx,
            result: None,
            f,
            _phantom: PhantomData,
        }
    }
    pub fn update_with_scalar_concrete(&mut self, input: &T, row_id: usize) -> Result<()> {
        self.result = (self.f)(
            self.result.as_ref().map(|x| x.as_scalar_ref()),
            input.value_at(row_id),
        )
        .map(|x| x.to_owned_scalar());
        Ok(())
    }
    pub fn update_concrete(&mut self, input: &T) -> Result<()> {
        let r = input
            .iter()
            .fold(self.result.as_ref().map(|x| x.as_scalar_ref()), &mut self.f)
            .map(|x| x.to_owned_scalar());
        self.result = r;
        Ok(())
    }
    pub fn output_concrete(&self, builder: &mut R::Builder) -> Result<()> {
        builder.append(self.result.as_ref().map(|x| x.as_scalar_ref()))
    }
    pub fn update_and_output_with_sorted_groups_concrete(
        &mut self,
        input: &T,
        builder: &mut R::Builder,
        groups: &EqGroups,
    ) -> Result<()> {
        let mut groups_iter = groups.get_starting_indices().iter().peekable();
        let mut cur = self.result.as_ref().map(|x| x.as_scalar_ref());
        for (i, v) in input.iter().enumerate() {
            if groups_iter.peek() == Some(&&i) {
                groups_iter.next();
                builder.append(cur)?;
                cur = None;
            }
            cur = (self.f)(cur, v);
        }
        self.result = cur.map(|x| x.to_owned_scalar());
        Ok(())
    }
}
/// Essentially `RTFn` is an alias of the specific Fn. It was aliased not to
/// shorten the `where` clause of `GeneralAgg`, but to workaround an compiler
/// error[E0582]: binding for associated type `Output` references lifetime `'a`,
/// which does not appear in the trait input types.
pub trait RTFn<'a, T, R>:
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
            fn return_type(&self) -> DataType {
                self.return_type.clone()
            }
            fn update_with_row(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
                if let ArrayImpl::$input_variant(i) =
                    input.column_at(self.input_col_idx)?.array_ref()
                {
                    self.update_with_scalar_concrete(i, row_id)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Input fail to match {}.",
                        stringify!($input_variant)
                    ))
                    .into())
                }
            }

            fn update(&mut self, input: &DataChunk) -> Result<()> {
                if let ArrayImpl::$input_variant(i) =
                    input.column_at(self.input_col_idx)?.array_ref()
                {
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
            fn update_and_output_with_sorted_groups(
                &mut self,
                input: &DataChunk,
                builder: &mut ArrayBuilderImpl,
                groups: &EqGroups,
            ) -> Result<()> {
                if let (ArrayImpl::$input_variant(i), ArrayBuilderImpl::$result_variant(b)) =
                    (input.column_at(self.input_col_idx)?.array_ref(), builder)
                {
                    self.update_and_output_with_sorted_groups_concrete(i, b, groups)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Input fail to match {} or builder fail to match {}.",
                        stringify!($input_variant),
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
impl_aggregator! { DecimalArray, Decimal, DecimalArray, Decimal }
impl_aggregator! { Utf8Array, Utf8, Utf8Array, Utf8 }
impl_aggregator! { I16Array, Int16, I64Array, Int64 }
impl_aggregator! { I32Array, Int32, I64Array, Int64 }
impl_aggregator! { F32Array, Float32, I64Array, Int64 }
impl_aggregator! { F64Array, Float64, I64Array, Int64 }
impl_aggregator! { DecimalArray, Decimal, I64Array, Int64 }
impl_aggregator! { Utf8Array, Utf8, I64Array, Int64 }
impl_aggregator! { BoolArray, Bool, I64Array, Int64 }
impl_aggregator! { I64Array, Int64, DecimalArray, Decimal }

use std::convert::From;
use std::ops::Add;

pub fn sum<R, T>(result: Option<R>, input: Option<T>) -> Option<R>
where
    R: From<T> + Add<Output = R> + Copy,
{
    match (result, input) {
        (_, None) => result,
        (None, Some(i)) => Some(R::from(i)),
        (Some(r), Some(i)) => Some(r + R::from(i)),
    }
}

pub fn min<'a, T>(result: Option<T>, input: Option<T>) -> Option<T>
where
    T: ScalarRef<'a> + PartialOrd,
{
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(if r < i { r } else { i }),
    }
}

pub fn min_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Option<&'a str> {
    min(r, i)
}

pub fn max<'a, T>(result: Option<T>, input: Option<T>) -> Option<T>
where
    T: ScalarRef<'a> + PartialOrd,
{
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(if r > i { r } else { i }),
    }
}

pub fn max_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Option<&'a str> {
    max(r, i)
}

/// create table t(v1 int);
/// insert into t values (null);
/// select count(*) from t; gives 1.
/// select count(v1) from t; gives 0.
/// select sum(v1) from t; gives null
pub fn count<T>(result: Option<i64>, input: Option<T>) -> Option<i64> {
    match (result, input) {
        (None, None) => Some(0),
        (Some(r), None) => Some(r),
        (None, Some(_)) => Some(1),
        (Some(r), Some(_)) => Some(r + 1),
    }
}

pub fn count_str(r: Option<i64>, i: Option<&str>) -> Option<i64> {
    count(r, i)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::array::column::Column;
    use crate::expr::AggKind;
    use crate::types::Decimal;
    use crate::vector_op::agg::aggregator::create_agg_state_unary;

    fn eval_agg(
        input_type: DataType,
        input: ArrayRef,
        agg_type: &AggKind,
        return_type: DataType,
        mut builder: ArrayBuilderImpl,
    ) -> Result<ArrayImpl> {
        let input_chunk = DataChunk::builder()
            .columns(vec![Column::new(input)])
            .build();
        let mut agg_state = create_agg_state_unary(input_type, 0, agg_type, return_type)?;
        agg_state.update(&input_chunk)?;
        agg_state.output(&mut builder)?;
        builder.finish()
    }

    #[test]
    fn test_create_agg_state() {
        let int64_type = DataType::Int64;
        let decimal_type = DataType::decimal_default();
        let bool_type = DataType::Boolean;
        let char_type = DataType::Char;

        macro_rules! test_create {
            ($input_type:expr, $agg:ident, $return_type:expr, $expected:ident) => {
                assert!(create_agg_state_unary(
                    $input_type.clone(),
                    0,
                    &AggKind::$agg,
                    $return_type.clone()
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
        test_create! { bool_type, Min, bool_type, is_err }
        test_create! { char_type, Min, char_type, is_ok }
    }

    #[test]
    fn vec_sum_int32() -> Result<()> {
        let input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        let agg_type = AggKind::Sum;
        let input_type = DataType::Int32;
        let return_type = DataType::Int64;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)?),
        )?;
        let actual = actual.as_int64();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(6)]);
        Ok(())
    }

    #[test]
    fn vec_sum_int64() -> Result<()> {
        let input = I64Array::from_slice(&[Some(1), Some(2), Some(3)])?;
        let agg_type = AggKind::Sum;
        let input_type = DataType::Int64;
        let return_type = DataType::decimal_default();
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            &agg_type,
            return_type,
            DecimalArrayBuilder::new(0)?.into(),
        )?;
        let actual: &DecimalArray = (&actual).into();
        let actual = actual.iter().collect::<Vec<Option<Decimal>>>();
        assert_eq!(actual, vec![Some(Decimal::from(6))]);
        Ok(())
    }

    #[test]
    fn vec_min_float32() -> Result<()> {
        let input =
            F32Array::from_slice(&[Some(1.0.into()), Some(2.0.into()), Some(3.0.into())]).unwrap();
        let agg_type = AggKind::Min;
        let input_type = DataType::Float32;
        let return_type = DataType::Float32;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Float32(F32ArrayBuilder::new(0)?),
        )?;
        let actual = actual.as_float32();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, &[Some(1.0.into())]);
        Ok(())
    }

    #[test]
    fn vec_min_char() -> Result<()> {
        let input = Utf8Array::from_slice(&[Some("b"), Some("aa")])?;
        let agg_type = AggKind::Min;
        let input_type = DataType::Char;
        let return_type = DataType::Char;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0)?),
        )?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("aa")]);
        Ok(())
    }

    #[test]
    fn vec_max_char() -> Result<()> {
        let input = Utf8Array::from_slice(&[Some("b"), Some("aa")])?;
        let agg_type = AggKind::Max;
        let input_type = DataType::Varchar;
        let return_type = DataType::Varchar;
        let actual = eval_agg(
            input_type,
            Arc::new(input.into()),
            &agg_type,
            return_type,
            ArrayBuilderImpl::Utf8(Utf8ArrayBuilder::new(0)?),
        )?;
        let actual = actual.as_utf8();
        let actual = actual.iter().collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("b")]);
        Ok(())
    }

    #[test]
    fn vec_count_int32() -> Result<()> {
        let test_case = |input: ArrayImpl, expected: &[Option<i64>]| -> Result<()> {
            let agg_type = AggKind::Count;
            let input_type = DataType::Int32;
            let return_type = DataType::Int64;
            let actual = eval_agg(
                input_type,
                Arc::new(input),
                &agg_type,
                return_type,
                ArrayBuilderImpl::Int64(I64ArrayBuilder::new(0)?),
            )?;
            let actual = actual.as_int64();
            let actual = actual.iter().collect::<Vec<_>>();
            assert_eq!(actual, expected);
            Ok(())
        };
        let input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        let expected = &[Some(3)];
        test_case(input.into(), expected)?;
        let input = I32Array::from_slice(&[]).unwrap();
        let expected = &[None];
        test_case(input.into(), expected)?;
        let input = I32Array::from_slice(&[None]).unwrap();
        let expected = &[Some(0)];
        test_case(input.into(), expected)
    }
}
