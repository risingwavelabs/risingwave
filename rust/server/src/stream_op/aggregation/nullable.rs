//! This module implements `NullableAgg`.

use std::marker::PhantomData;

use crate::array2::*;
use crate::buffer::Bitmap;
use crate::error::Result;
use crate::stream_op::{Op, Ops};

use super::{StreamingAggFunction, StreamingAggState, StreamingAggStateImpl};

/// `NullableAgg` count rows. If there is zero rows in the current state, `null` will be emitted
/// instead of the concrete value.
///
/// * `I`: Type of input array.
/// * `O`: Type of output array builder.
/// * `S`: Type of inner state
pub struct NullableAgg<I, O, S>
where
    I: Array,
    O: ArrayBuilder,
    S: StreamingAggState<I> + StreamingAggFunction<O>,
{
    inner: S,
    row_cnt: i64,
    _phantom: PhantomData<(I, O)>,
}

impl<I, O, S> NullableAgg<I, O, S>
where
    I: Array,
    O: ArrayBuilder,
    S: StreamingAggState<I> + StreamingAggFunction<O>,
{
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            row_cnt: 0,
            _phantom: PhantomData,
        }
    }
}

impl<I, O, S> StreamingAggState<I> for NullableAgg<I, O, S>
where
    I: Array,
    O: ArrayBuilder,
    S: StreamingAggState<I> + StreamingAggFunction<O>,
{
    fn apply_batch_concrete(
        &mut self,
        ops: Ops<'_>,
        skip: Option<&Bitmap>,
        data: &I,
    ) -> Result<()> {
        for op in ops {
            match op {
                Op::Insert | Op::UpdateInsert => self.row_cnt += 1,
                Op::Delete | Op::UpdateDelete => self.row_cnt -= 1,
            }
        }
        self.inner.apply_batch_concrete(ops, skip, data)?;
        Ok(())
    }
}

impl<I, O, S> StreamingAggFunction<O> for NullableAgg<I, O, S>
where
    I: Array,
    O: ArrayBuilder,
    S: StreamingAggState<I> + StreamingAggFunction<O>,
{
    fn get_output_concrete(&self, builder: &mut O) -> Result<()> {
        if self.row_cnt == 0 {
            builder.append(None)?;
        } else {
            self.inner.get_output_concrete(builder)?;
        }
        Ok(())
    }
}

macro_rules! impl_agg {
  ($result:tt, $result_variant:tt, $input:tt) => {
    impl<S> StreamingAggStateImpl for NullableAgg<$input, $result, S>
    where
      S: StreamingAggState<$input> + StreamingAggFunction<$result> + Send + Sync,
    {
      fn apply_batch(
        &mut self,
        ops: Ops<'_>,
        skip: Option<&Bitmap>,
        data: &ArrayImpl,
      ) -> Result<()> {
        self.apply_batch_concrete(ops, skip, data.into())
      }

      fn get_output(&self, builder: &mut ArrayBuilderImpl) -> Result<()> {
        match builder {
          ArrayBuilderImpl::$result_variant(builder) => self.get_output_concrete(builder),
          other_variant => panic!(
            "type mismatch in streaming aggregator StreamingFoldAgg output: expected {}, get {}",
            stringify!($result),
            other_variant.get_ident()
          ),
        }
      }

      fn new_builder(&self) -> ArrayBuilderImpl {
        ArrayBuilderImpl::$result_variant($result::new(0).unwrap())
      }
    }
  };
}

impl_agg! { I16ArrayBuilder, Int16, I16Array }
impl_agg! { I32ArrayBuilder, Int32, I32Array }
impl_agg! { I64ArrayBuilder, Int64, I64Array }
impl_agg! { F32ArrayBuilder, Float32, F32Array }
impl_agg! { F64ArrayBuilder, Float64, F64Array }

#[cfg(test)]
mod tests {
    use crate::array2::{I64Array, I64ArrayBuilder};

    use super::super::tests::get_output_from_state;
    use super::*;

    struct TestState {}

    impl StreamingAggState<I64Array> for TestState {
        fn apply_batch_concrete(
            &mut self,
            _ops: Ops<'_>,
            _skip: Option<&Bitmap>,
            _data: &I64Array,
        ) -> Result<()> {
            Ok(())
        }
    }

    impl StreamingAggFunction<I64ArrayBuilder> for TestState {
        fn get_output_concrete(&self, builder: &mut I64ArrayBuilder) -> Result<()> {
            builder.append(Some(233))?;
            Ok(())
        }
    }

    #[test]
    fn test_nullable_state() {
        let mut state = NullableAgg::new(TestState {});

        // when there is no element, output should be `None`.
        assert_eq!(
            get_output_from_state(&mut state).iter().collect::<Vec<_>>(),
            [None]
        );

        // insert one element to state
        state
            .apply_batch_concrete(&[Op::Insert], None, &I64Array::from_slice(&[None]).unwrap())
            .unwrap();

        // should be the same as `TestState`'s output
        assert_eq!(
            get_output_from_state(&mut state).iter().collect::<Vec<_>>(),
            [Some(233)]
        );

        // delete one element from state
        state
            .apply_batch_concrete(&[Op::Delete], None, &I64Array::from_slice(&[None]).unwrap())
            .unwrap();

        // should be `None`.
        assert_eq!(
            get_output_from_state(&mut state).iter().collect::<Vec<_>>(),
            [None]
        );

        // one more deletion, so we are having `-1` elements inside.
        state
            .apply_batch_concrete(&[Op::Delete], None, &I64Array::from_slice(&[None]).unwrap())
            .unwrap();

        // should be the same as `TestState`'s output
        assert_eq!(
            get_output_from_state(&mut state).iter().collect::<Vec<_>>(),
            [Some(233)]
        );
    }
}
