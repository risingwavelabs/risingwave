use std::marker::PhantomData;

use risingwave_pb::expr::AggCall;

use crate::array::*;
use crate::error::{ErrorCode, Result};
use crate::expr::AggKind;
use crate::types::*;

/// An `Aggregator` supports `update` data and `output` result.
pub trait Aggregator: Send + 'static {
    fn return_type(&self) -> DataTypeKind;

    /// `update` the aggregator with a row with type checked at runtime.
    fn update_with_row(&mut self, input: &DataChunk, row_id: usize) -> Result<()>;
    /// `update` the aggregator with `Array` with input with type checked at runtime.
    ///
    /// This may be deprecated as it consumes whole array without sort or hash group info.
    fn update(&mut self, input: &DataChunk) -> Result<()>;

    /// `output` the aggregator to `ArrayBuilder` with input with type checked at runtime.
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;

    /// `update_and_output_with_sorted_groups` supersede `update` when grouping with the sort
    /// aggregate algorithm.
    ///
    /// Rather than updating with the whole `input` array all at once, it updates with each
    /// subslice of the `input` array according to the `EqGroups`. Finished groups are outputted
    /// to `builder` immediately along the way. After this call, the internal state is about
    /// the last group which may continue in the next chunk. It can be obtained with `output` when
    /// there are no more upstream data.
    fn update_and_output_with_sorted_groups(
        &mut self,
        input: &DataChunk,
        builder: &mut ArrayBuilderImpl,
        groups: &EqGroups,
    ) -> Result<()>;
}

pub type BoxedAggState = Box<dyn Aggregator>;

pub struct AggStateFactory {
    // When agg func is count(*), the args is empty and input type is None.
    input_type: Option<DataTypeKind>,
    input_col_idx: usize,
    agg_kind: AggKind,
    return_type: DataTypeKind,
}

impl AggStateFactory {
    pub fn new(prost: &AggCall) -> Result<Self> {
        let return_type = DataTypeKind::from(prost.get_return_type()?);
        let agg_kind = AggKind::try_from(prost.get_type()?)?;
        match &prost.get_args()[..] {
            [ref arg] => {
                let input_type = DataTypeKind::from(arg.get_type()?);
                let input_col_idx = arg.get_input()?.get_column_idx() as usize;
                Ok(Self {
                    input_type: Some(input_type),
                    input_col_idx,
                    agg_kind,
                    return_type,
                })
            }
            [] => match (&agg_kind, return_type) {
                (AggKind::Count, DataTypeKind::Int64) => Ok(Self {
                    input_type: None,
                    input_col_idx: 0,
                    agg_kind,
                    return_type,
                }),
                _ => Err(ErrorCode::InternalError(format!(
                    "Agg {:?} without args not supported",
                    agg_kind
                ))
                .into()),
            },
            _ => Err(
                ErrorCode::InternalError("Agg with more than 1 input not supported.".into()).into(),
            ),
        }
    }

    pub fn create_agg_state(&self) -> Result<Box<dyn Aggregator>> {
        if let Some(input_type) = self.input_type {
            create_agg_state_unary(
                input_type,
                self.input_col_idx,
                &self.agg_kind,
                self.return_type,
            )
        } else {
            Ok(Box::new(CountStar {
                return_type: self.return_type,
                result: 0,
            }))
        }
    }

    pub fn get_return_type(&self) -> DataTypeKind {
        self.return_type
    }
}

fn create_agg_state_unary(
    input_type: DataTypeKind,
    input_col_idx: usize,
    agg_type: &AggKind,
    return_type: DataTypeKind,
) -> Result<Box<dyn Aggregator>> {
    use crate::expr::data_types::*;

    macro_rules! gen_arms {
    [$(($agg:ident, $fn:expr, $in:tt, $ret:tt)),* $(,)?] => {
      match (
        input_type,
        agg_type,
        return_type,
      ) {
        $(
        ($in! { type_match_pattern }, AggKind::$agg, $ret! { type_match_pattern }) => {
          Box::new(GeneralAgg::<$in! { type_array }, _, $ret! { type_array }>::new(
            return_type,
            input_col_idx,
            $fn,
          ))
        }
        )*
        (unimpl_input, unimpl_agg, unimpl_ret) => {
          return Err(
            ErrorCode::InternalError(format!(
              "unsupported aggregator: type={:?} input={:?} output={:?}",
              unimpl_agg, unimpl_input, unimpl_ret
            ))
            .into(),
          )
        }
      }
    };
  }
    let state: Box<dyn Aggregator> = gen_arms![
        (Count, count, int16, int64),
        (Count, count, int32, int64),
        (Count, count, int64, int64),
        (Count, count, float32, int64),
        (Count, count, float64, int64),
        (Count, count, decimal, int64),
        (Count, count_str, char, int64),
        (Count, count_str, varchar, int64),
        (Count, count, boolean, int64),
        (Sum, sum, int16, int64),
        (Sum, sum, int32, int64),
        (Sum, sum, int64, decimal),
        (Sum, sum, float32, float32),
        (Sum, sum, float64, float64),
        (Sum, sum, decimal, decimal),
        (Min, min, int16, int16),
        (Min, min, int32, int32),
        (Min, min, int64, int64),
        (Min, min, float32, float32),
        (Min, min, float64, float64),
        (Min, min, decimal, decimal),
        (Min, min_str, char, char),
        (Min, min_str, varchar, varchar),
        (Max, max, int16, int16),
        (Max, max, int32, int32),
        (Max, max, int64, int64),
        (Max, max, float32, float32),
        (Max, max, float64, float64),
        (Max, max, decimal, decimal),
        (Max, max_str, char, char),
        (Max, max_str, varchar, varchar),
        // Global Agg
        (Sum, sum, int64, int64),
    ];
    Ok(state)
}

struct CountStar {
    return_type: DataTypeKind,
    result: usize,
}
impl Aggregator for CountStar {
    fn return_type(&self) -> DataTypeKind {
        self.return_type
    }
    fn update(&mut self, input: &DataChunk) -> Result<()> {
        self.result += input.cardinality();
        Ok(())
    }
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        match builder {
            ArrayBuilderImpl::Int64(b) => b.append(Some(self.result as i64)),
            _ => Err(ErrorCode::InternalError("Unexpected builder for count(*).".into()).into()),
        }
    }
    fn update_and_output_with_sorted_groups(
        &mut self,
        input: &DataChunk,
        builder: &mut ArrayBuilderImpl,
        groups: &EqGroups,
    ) -> Result<()> {
        let builder = match builder {
            ArrayBuilderImpl::Int64(b) => b,
            _ => {
                return Err(
                    ErrorCode::InternalError("Unexpected builder for count(*).".into()).into(),
                )
            }
        };
        // The first element continues the same group in `self.result`. The following
        // groups' sizes are simply distance between group start indices. The distance
        // between last element and `input.cardinality()` is the ongoing group that
        // may continue in following chunks.
        let mut groups_iter = groups.0.iter();
        if let Some(first) = groups_iter.next() {
            builder.append(Some((self.result + first) as i64))?;
            let mut prev = first;
            for g in groups_iter {
                builder.append(Some((g - prev) as i64))?;
                prev = g;
            }
            self.result = input.cardinality() - prev;
        } else {
            self.result += input.cardinality();
        }
        Ok(())
    }

    fn update_with_row(&mut self, input: &DataChunk, row_id: usize) -> Result<()> {
        if let Some(visibility) = input.visibility() {
            if visibility.is_set(row_id)? {
                self.result += 1;
            }
        } else {
            self.result += 1;
        }
        Ok(())
    }
}

struct GeneralAgg<T, F, R>
where
    T: Array,
    F: Send + for<'a> RTFn<'a, T, R>,
    R: Array,
{
    return_type: DataTypeKind,
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
    fn new(return_type: DataTypeKind, input_col_idx: usize, f: F) -> Self {
        Self {
            return_type,
            input_col_idx,
            result: None,
            f,
            _phantom: PhantomData,
        }
    }
    fn update_with_scalar_concrete(&mut self, input: &T, row_id: usize) -> Result<()> {
        self.result = (self.f)(
            self.result.as_ref().map(|x| x.as_scalar_ref()),
            input.value_at(row_id),
        )
        .map(|x| x.to_owned_scalar());
        Ok(())
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
    fn update_and_output_with_sorted_groups_concrete(
        &mut self,
        input: &T,
        builder: &mut R::Builder,
        groups: &EqGroups,
    ) -> Result<()> {
        let mut groups_iter = groups.0.iter().peekable();
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
            fn return_type(&self) -> DataTypeKind {
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

fn max<'a, T>(result: Option<T>, input: Option<T>) -> Option<T>
where
    T: ScalarRef<'a> + PartialOrd,
{
    match (result, input) {
        (None, _) => input,
        (_, None) => result,
        (Some(r), Some(i)) => Some(if r > i { r } else { i }),
    }
}

fn max_str<'a>(r: Option<&'a str>, i: Option<&'a str>) -> Option<&'a str> {
    max(r, i)
}

/// create table t(v1 int);
/// insert into t values (null);
/// select count(*) from t; gives 1.
/// select count(v1) from t; gives 0.
/// select sum(v1) from t; gives null
fn count<T>(result: Option<i64>, input: Option<T>) -> Option<i64> {
    match (result, input) {
        (None, None) => Some(0),
        (Some(r), None) => Some(r),
        (None, Some(_)) => Some(1),
        (Some(r), Some(_)) => Some(r + 1),
    }
}

fn count_str(r: Option<i64>, i: Option<&str>) -> Option<i64> {
    count(r, i)
}

/// `EqGroups` encodes the grouping information in the sort aggregate algorithm.
///
/// - `SortedGrouper::split_groups` creates a `EqGroups` from a single column.
/// - `EqGroups::intersect` combines `EqGroups` from each column into a single one.
/// - `{SortedGrouper,Aggregator}::update_and_output_with_sorted_groups` needs the
/// grouping information to perform the grouped aggregation.
///
/// Internally, `EqGroups` is encoded as the indices that each new group starts.
/// Specially, a leading `0` means (the 0-th tuple of) this chunk starts a new
/// group compared to (the last tuple of) the previous chunk, and there is no leading
/// `0` when it continues the same group or there is no previous chunk.
pub struct EqGroups(Vec<usize>);
impl EqGroups {
    /// `intersect` combines the grouping information from each column into a single one.
    /// This is required so that we know `group by c1, c2` with `c1 = [a, a, c, c, d, d]`
    /// and `c2 = [g, h, h, h, h, h]` actually forms 4 groups: `[(a, g), (a, h), (c, h), (d, h)]`.
    ///
    /// Since the internal encoding is a sequence of sorted indices, this is effectively
    /// merging all sequences into a single one with deduplication. In the example above,
    /// the `EqGroups` of `c1` is `[2, 4]` and that of `c2` is `[1]`, so the output of
    /// `intersect` would be `[1, 2, 4]` identifying the new groups starting at these indices.
    pub fn intersect(columns: &[EqGroups]) -> EqGroups {
        let mut ret = Vec::new();
        // Use of BinaryHeap here is not to get a performant implementation but a
        // concise one. The number of group columns would not be huge.
        // Storing iterator rather than (ci, idx) in heap actually makes the implementation
        // more verbose:
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=1e3b098ee3ef352d5a0cac03b3193799
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut heap = BinaryHeap::new();
        for (ci, column) in columns.iter().enumerate() {
            if let Some(ri) = column.0.get(0) {
                heap.push(Reverse((ri, ci, 0)));
            }
        }
        while let Some(Reverse((ri, ci, idx))) = heap.pop() {
            if let Some(ri_next) = columns[ci].0.get(idx + 1) {
                heap.push(Reverse((ri_next, ci, idx + 1)));
            }
            if ret.last() == Some(ri) {
                continue;
            }
            ret.push(*ri);
        }
        EqGroups(ret)
    }
}

/// `SortedGrouper` contains the state of a group column in the sort aggregate
/// algorithm, just like `Aggregator` contains the state of an aggregate column.
pub trait SortedGrouper: Send + 'static {
    /// `split_groups` detects the `EqGroups` from the `input` array if appended
    /// to current state. See the documentation of `EqGroups` to learn more.
    ///
    /// This is a `dry-run` and does not update its state yet, because it does not
    /// have grouping information from all group columns yet.
    fn split_groups(&self, input: &ArrayImpl) -> Result<EqGroups>;

    /// `update_and_output_with_sorted_groups` updates with each subslice of the
    /// `input` array according to the `EqGroups`. Finished groups are outputted
    /// to `builder` immediately along the way. After this call, the internal state
    /// is about the last group which may continue in the next chunk. It can be
    /// obtained with `output` when there are no more upstream data.
    fn update_and_output_with_sorted_groups(
        &mut self,
        input: &ArrayImpl,
        builder: &mut ArrayBuilderImpl,
        groups: &EqGroups,
    ) -> Result<()>;

    /// `output` the state to the `builder`. Expected to be called once to obtain
    /// the last group, when there are no more upstream data.
    fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()>;
}
pub type BoxedSortedGrouper = Box<dyn SortedGrouper>;

pub fn create_sorted_grouper(input_type: DataTypeKind) -> Result<BoxedSortedGrouper> {
    match input_type {
        // DataTypeKind::Int16 => Ok(Box::new(GeneralSortedGrouper::<I16Array>::new())),
        // DataTypeKind::Int32 => Ok(Box::new(GeneralSortedGrouper::<I32Array>::new())),
        DataTypeKind::Int32 => Ok(Box::new(GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        })),
        // DataTypeKind::Int64 => Ok(Box::new(GeneralSortedGrouper::new::<I64Array>())),
        unimpl_input => todo!("unsupported sorted grouper: input={:?}", unimpl_input),
    }
}

struct GeneralSortedGrouper<T>
where
    T: Array,
    for<'a> T::RefItem<'a>: Eq,
{
    // Technically `group_value` is meaningless when `ongoing == false` and this
    // should be a single `Option<Option<T:OwnedItem>>`. But it actually makes this
    // simple struct less readable.
    ongoing: bool,
    group_value: Option<T::OwnedItem>,
}
impl<T> GeneralSortedGrouper<T>
where
    T: Array,
    for<'a> T::RefItem<'a>: Eq,
{
    fn new() -> Self {
        Self {
            ongoing: false,
            group_value: None,
        }
    }
    fn split_groups_concrete(&self, input: &T) -> Result<EqGroups> {
        let mut ret = Vec::new();
        let mut ongoing = self.ongoing;
        let mut ongoing_group = self.group_value.as_ref().map(|x| x.as_scalar_ref());
        for (i, v) in input.iter().enumerate() {
            if ongoing && ongoing_group == v {
                continue;
            }
            if ongoing {
                ret.push(i);
            }
            ongoing = true;
            ongoing_group = v;
        }
        Ok(EqGroups(ret))
    }
    fn update_and_output_with_sorted_groups_concrete(
        &mut self,
        input: &T,
        builder: &mut T::Builder,
        groups: &EqGroups,
    ) -> Result<()> {
        let mut groups_iter = groups.0.iter().peekable();
        let mut cur = self.group_value.as_ref().map(|x| x.as_scalar_ref());
        for (i, v) in input.iter().enumerate() {
            if groups_iter.peek() == Some(&&i) {
                groups_iter.next();
                ensure!(self.ongoing);
                builder.append(cur)?;
            }
            self.ongoing = true;
            cur = v;
        }
        self.group_value = cur.map(|x| x.to_owned_scalar());
        Ok(())
    }
    fn output_concrete(&self, builder: &mut T::Builder) -> Result<()> {
        builder.append(self.group_value.as_ref().map(|x| x.as_scalar_ref()))
    }
}

macro_rules! impl_sorted_grouper {
    ($input:ty, $input_variant:ident) => {
        impl SortedGrouper for GeneralSortedGrouper<$input> {
            fn split_groups(&self, input: &ArrayImpl) -> Result<EqGroups> {
                if let ArrayImpl::$input_variant(i) = input {
                    self.split_groups_concrete(i)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Input fail to match {}.",
                        stringify!($input_variant)
                    ))
                    .into())
                }
            }
            fn output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
                if let ArrayBuilderImpl::$input_variant(b) = builder {
                    self.output_concrete(b)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Builder fail to match {}.",
                        stringify!($input_variant)
                    ))
                    .into())
                }
            }
            fn update_and_output_with_sorted_groups(
                &mut self,
                input: &ArrayImpl,
                builder: &mut ArrayBuilderImpl,
                groups: &EqGroups,
            ) -> Result<()> {
                if let (ArrayImpl::$input_variant(i), ArrayBuilderImpl::$input_variant(b)) =
                    (input, builder)
                {
                    self.update_and_output_with_sorted_groups_concrete(i, b, groups)
                } else {
                    Err(ErrorCode::InternalError(format!(
                        "Input fail to match {} or builder fail to match {}.",
                        stringify!($input_variant),
                        stringify!($input_variant)
                    ))
                    .into())
                }
            }
        }
    };
}
impl_sorted_grouper! { I16Array, Int16 }
impl_sorted_grouper! { I32Array, Int32 }
impl_sorted_grouper! { I64Array, Int64 }

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType as ProstDataType;
    use risingwave_pb::expr::agg_call::Type;
    use risingwave_pb::expr::AggCall;

    use super::*;
    use crate::array::column::Column;
    use crate::types::Decimal;

    fn eval_agg(
        input_type: DataTypeKind,
        input: ArrayRef,
        agg_type: &AggKind,
        return_type: DataTypeKind,
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
        let int64_type = DataTypeKind::Int64;
        let decimal_type = DataTypeKind::decimal_default();
        let bool_type = DataTypeKind::Boolean;
        let char_type = DataTypeKind::Char;

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
        let input_type = DataTypeKind::Int32;
        let return_type = DataTypeKind::Int64;
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
        let input_type = DataTypeKind::Int64;
        let return_type = DataTypeKind::decimal_default();
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
        let input_type = DataTypeKind::Float32;
        let return_type = DataTypeKind::Float32;
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
        let input_type = DataTypeKind::Char;
        let return_type = DataTypeKind::Char;
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
        let input_type = DataTypeKind::Varchar;
        let return_type = DataTypeKind::Varchar;
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
            let input_type = DataTypeKind::Int32;
            let return_type = DataTypeKind::Int64;
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

    #[test]
    fn group_int32() -> Result<()> {
        let mut g = GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        };
        let mut builder = I32ArrayBuilder::new(0)?;

        let input = I32Array::from_slice(&[Some(1), Some(1), Some(3)]).unwrap();
        let eq = g.split_groups_concrete(&input)?;
        g.update_and_output_with_sorted_groups_concrete(&input, &mut builder, &eq)?;
        assert_eq!(eq.0, vec![2]);
        let input = I32Array::from_slice(&[Some(3), Some(4), Some(4)]).unwrap();
        let eq = g.split_groups_concrete(&input)?;
        g.update_and_output_with_sorted_groups_concrete(&input, &mut builder, &eq)?;
        assert_eq!(eq.0, vec![1]);

        g.output_concrete(&mut builder)?;
        assert_eq!(
            builder.finish()?.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(4)]
        );
        Ok(())
    }

    #[test]
    fn group_intersect() {
        assert_eq!(
            EqGroups::intersect(&[EqGroups(vec![0, 2, 4]), EqGroups(vec![1, 2, 5]),]).0,
            vec![0, 1, 2, 4, 5]
        );
    }

    #[test]
    fn vec_agg_group() -> Result<()> {
        let mut g0 = GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        };
        let mut g0_builder = I32ArrayBuilder::new(0)?;
        let mut g1 = GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        };
        let mut g1_builder = I32ArrayBuilder::new(0)?;
        let mut a = GeneralAgg::<I32Array, _, I64Array>::new(DataTypeKind::Int64, 0, sum);
        let mut a_builder = I64ArrayBuilder::new(0)?;

        let g0_input = I32Array::from_slice(&[Some(1), Some(1), Some(3)]).unwrap();
        let eq0 = g0.split_groups_concrete(&g0_input)?;
        let g1_input = I32Array::from_slice(&[Some(7), Some(8), Some(8)]).unwrap();
        let eq1 = g1.split_groups_concrete(&g1_input)?;
        let eq = EqGroups::intersect(&[eq0, eq1]);
        g0.update_and_output_with_sorted_groups_concrete(&g0_input, &mut g0_builder, &eq)?;
        g1.update_and_output_with_sorted_groups_concrete(&g1_input, &mut g1_builder, &eq)?;
        let a_input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        a.update_and_output_with_sorted_groups_concrete(&a_input, &mut a_builder, &eq)?;

        let g0_input = I32Array::from_slice(&[Some(3), Some(4), Some(4)]).unwrap();
        let eq0 = g0.split_groups_concrete(&g0_input)?;
        let g1_input = I32Array::from_slice(&[Some(8), Some(8), Some(8)]).unwrap();
        let eq1 = g1.split_groups_concrete(&g1_input)?;
        let eq = EqGroups::intersect(&[eq0, eq1]);
        g0.update_and_output_with_sorted_groups_concrete(&g0_input, &mut g0_builder, &eq)?;
        g1.update_and_output_with_sorted_groups_concrete(&g1_input, &mut g1_builder, &eq)?;
        let a_input = I32Array::from_slice(&[Some(1), Some(2), Some(3)]).unwrap();
        a.update_and_output_with_sorted_groups_concrete(&a_input, &mut a_builder, &eq)?;

        g0.output_concrete(&mut g0_builder)?;
        g1.output_concrete(&mut g1_builder)?;
        a.output_concrete(&mut a_builder)?;
        assert_eq!(
            g0_builder.finish()?.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(1), Some(3), Some(4)]
        );
        assert_eq!(
            g1_builder.finish()?.iter().collect::<Vec<_>>(),
            vec![Some(7), Some(8), Some(8), Some(8)]
        );
        assert_eq!(
            a_builder.finish()?.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(4), Some(5)]
        );
        Ok(())
    }

    #[test]
    fn vec_count_star() {
        let mut g0 = GeneralSortedGrouper::<I32Array> {
            ongoing: false,
            group_value: None,
        };
        let mut g0_builder = I32ArrayBuilder::new(0).unwrap();
        let prost = AggCall {
            r#type: Type::Count as i32,
            args: vec![],
            return_type: Some(ProstDataType {
                type_name: TypeName::Int64 as i32,
                ..Default::default()
            }),
        };
        let mut a = AggStateFactory::new(&prost)
            .unwrap()
            .create_agg_state()
            .unwrap();
        let mut a_builder = a.return_type().create_array_builder(0).unwrap();

        let input = I32Array::from_slice(&[Some(1), Some(1), Some(3)]).unwrap();
        let eq = g0.split_groups_concrete(&input).unwrap();
        g0.update_and_output_with_sorted_groups_concrete(&input, &mut g0_builder, &eq)
            .unwrap();
        a.update_and_output_with_sorted_groups(
            &DataChunk::builder()
                .columns(vec![Column::new(Arc::new(input.into()))])
                .build(),
            &mut a_builder,
            &eq,
        )
        .unwrap();

        let input = I32Array::from_slice(&[Some(3), Some(3), Some(3)]).unwrap();
        let eq = g0.split_groups_concrete(&input).unwrap();
        g0.update_and_output_with_sorted_groups_concrete(&input, &mut g0_builder, &eq)
            .unwrap();
        a.update_and_output_with_sorted_groups(
            &DataChunk::builder()
                .columns(vec![Column::new(Arc::new(input.into()))])
                .build(),
            &mut a_builder,
            &eq,
        )
        .unwrap();

        let input = I32Array::from_slice(&[Some(3), Some(4), Some(4)]).unwrap();
        let eq = g0.split_groups_concrete(&input).unwrap();
        g0.update_and_output_with_sorted_groups_concrete(&input, &mut g0_builder, &eq)
            .unwrap();
        a.update_and_output_with_sorted_groups(
            &DataChunk::builder()
                .columns(vec![Column::new(Arc::new(input.into()))])
                .build(),
            &mut a_builder,
            &eq,
        )
        .unwrap();

        g0.output_concrete(&mut g0_builder).unwrap();
        a.output(&mut a_builder).unwrap();
        assert_eq!(
            g0_builder.finish().unwrap().iter().collect::<Vec<_>>(),
            vec![Some(1), Some(3), Some(4)]
        );
        assert_eq!(
            a_builder
                .finish()
                .unwrap()
                .as_int64()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), Some(5), Some(2)]
        );
    }
}
