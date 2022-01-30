use risingwave_pb::expr::AggCall;

use crate::array::*;
use crate::error::{ErrorCode, Result};
use crate::expr::AggKind;
use crate::types::*;
use crate::vector_op::agg::count_star::CountStar;
use crate::vector_op::agg::general_agg::*;
use crate::vector_op::agg::general_sorted_grouper::EqGroups;

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
            Ok(Box::new(CountStar::new(self.return_type, 0)))
        }
    }

    pub fn get_return_type(&self) -> DataTypeKind {
        self.return_type
    }
}

pub fn create_agg_state_unary(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::column::Column;

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
}
