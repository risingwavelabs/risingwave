use crate::array2::{Array, ArrayBuilder, ArrayImpl, ArrayRef, DataChunk};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::{DataType, DataTypeRef, Scalar};
use itertools::multizip;
use paste::paste;
use std::sync::Arc;

macro_rules! gen_expr_normal {
  ($ty_name:ident,$($arg:ident),*) => {
    paste! {
      pub struct $ty_name<
        $($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($($arg::RefItem<'a>, )*) -> Result<OA::OwnedItem>,
      > {
        // pub the fields in super mod, so that we can construct it directly.
        // FIXME: make private while new function available.
        $(pub(super) [<expr_ $arg:lower>]: BoxedExpression,)*
        pub(super) return_type: DataTypeRef,
        pub(super) func: F,
        pub(super) data1: std::marker::PhantomData<($($arg, )* OA)>,
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($($arg::RefItem<'a>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
      >  Expression for $ty_name<$($arg, )* OA, F>
      where
        $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
        for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
      {
        fn return_type(&self) -> &dyn DataType {
          &*self.return_type
        }

        fn return_type_ref(&self) -> DataTypeRef {
          self.return_type.clone()
        }

        fn eval(&mut self, data_chunk: &DataChunk) -> Result<ArrayRef> {
          $(
            let [<ret_ $arg:lower>] = self.[<expr_ $arg:lower>].eval(data_chunk)?;
            let [<arr_ $arg:lower>]: &$arg = [<ret_ $arg:lower>].as_ref().into();
          )*

          let bitmap = data_chunk.get_visibility_ref();
          let mut output_array = <OA as Array>::Builder::new(data_chunk.capacity())?;
          Ok(Arc::new(match bitmap {
            Some(bitmap) => {
              for (($([<v_ $arg:lower>], )*), visible) in multizip(($([<arr_ $arg:lower>].iter(), )*)).zip(bitmap.iter()) {
                if !visible {
                  continue;
                }
                if let ($(Some([<v_ $arg:lower>]), )*) = ($([<v_ $arg:lower>], )*) {
                  let ret = (self.func)($([<v_ $arg:lower>], )*)?;
                  let output = Some(ret.as_scalar_ref());
                  output_array.append(output)?;
                } else {
                  output_array.append(None)?;
                }
              }
              output_array.finish()?.into()
            }
            None => {
              for ($([<v_ $arg:lower>], )*) in multizip(($([<arr_ $arg:lower>].iter(), )*)) {
                if let ($(Some([<v_ $arg:lower>]), )*) = ($([<v_ $arg:lower>], )*) {
                  let ret = (self.func)($([<v_ $arg:lower>], )*)?;
                  let output = Some(ret.as_scalar_ref());
                  output_array.append(output)?;
                } else {
                  output_array.append(None)?;
                }
              }
              output_array.finish()?.into()
            }
          }))
        }
      }

      impl<$($arg: Array, )*
        OA: Array,
        F: for<'a> Fn($($arg::RefItem<'a>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
      > $ty_name<$($arg, )* OA, F> {
        // Compile failed due to some GAT lifetime issues so make this field private.
        // Check issues #742.
        fn new(
          $([<expr_ $arg:lower>]: BoxedExpression, )*
          return_type: DataTypeRef,
          func: F,
        ) -> Self {
          Self {
            $([<expr_ $arg:lower>], )*
            return_type,
            func,
            data1: std::marker::PhantomData,
          }
        }
      }
    }
  }
}

gen_expr_normal!(UnaryExpression, IA1);
gen_expr_normal!(BinaryExpression, IA1, IA2);
gen_expr_normal!(TenaryExpression, IA1, IA2, IA3);
