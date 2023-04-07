// Copyright 2023 RisingWave Labs
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

//! Template macro to generate code for unary/binary/ternary expression.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use itertools::{multizip, Itertools};
use paste::paste;
use risingwave_common::array::{Array, ArrayBuilder, ArrayImpl, DataChunk, Utf8Array};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{option_as_scalar_ref, DataType, Datum, Scalar};
use risingwave_common::util::iter_util::ZipEqDebug;

use crate::expr::{BoxedExpression, Expression, ValueImpl, ValueRef};

macro_rules! gen_eval {
    { ($macro:ident, $macro_row:ident), $ty_name:ident, $OA:ty, $($arg:ident,)* } => {
        fn eval_new<'a, 'b, 'async_trait>(&'a self, data_chunk: &'b DataChunk)
            -> Pin<Box<dyn Future<Output = $crate::Result<ValueImpl>> + Send + 'async_trait>>
        where
            'a: 'async_trait,
            'b: 'async_trait,
        {
            Box::pin(async move { paste! {
                $(
                    let [<ret_ $arg:lower>] = self.[<expr_ $arg:lower>].eval_new(data_chunk).await?;
                    let [<val_ $arg:lower>]: ValueRef<'_, $arg> = (&[<ret_ $arg:lower>]).into();
                )*

                Ok(match ($([<val_ $arg:lower>], )*) {
                    // If all arguments are scalar, we can directly compute the result.
                    ($(ValueRef::Scalar { value: [<scalar_ref_ $arg:lower>], capacity: [<cap_ $arg:lower>] }, )*) => {
                        let output_scalar = $macro_row!(self, $([<scalar_ref_ $arg:lower>],)*);
                        let output_datum = output_scalar.map(|s| s.to_scalar_value());
                        let capacity = data_chunk.capacity();

                        if cfg!(debug_assertions) {
                            let all_capacities = [capacity, $([<cap_ $arg:lower>], )*];
                            assert!(all_capacities.into_iter().all_equal(), "capacities mismatched: {:?}", all_capacities);
                        }

                        ValueImpl::Scalar { value: output_datum, capacity }
                    }

                    // Otherwise, fallback to array computation.
                    // TODO: match all possible combinations to further get rid of the overhead of `Either` iterators.
                    ($([<val_ $arg:lower>], )*) => {
                        let bitmap = data_chunk.visibility();
                        let mut output_array = <$OA as Array>::Builder::with_meta(data_chunk.capacity(), (&self.return_type).into());
                        let array = match bitmap {
                            Some(bitmap) => {
                                // TODO: use `izip` here.
                                for (($([<v_ $arg:lower>], )*), visible) in multizip(($([<val_ $arg:lower>].iter(), )*)).zip_eq_debug(bitmap.iter()) {
                                    if !visible {
                                        output_array.append_null();
                                        continue;
                                    }
                                    $macro!(self, output_array, $([<v_ $arg:lower>],)*)
                                }
                                output_array.finish().into()
                            }
                            None => {
                                // TODO: use `izip` here.
                                for ($([<v_ $arg:lower>], )*) in multizip(($([<val_ $arg:lower>].iter(), )*)) {
                                    $macro!(self, output_array, $([<v_ $arg:lower>],)*)
                                }
                                output_array.finish().into()
                            }
                        };

                        ValueImpl::Array(Arc::new(array))
                    }
                })
            }})
        }

        /// `eval_row()` first calls `eval_row()` on the inner expressions to get the resulting datums,
        /// then directly calls `$macro_row` to evaluate the current expression.
        fn eval_row<'a, 'b, 'async_trait>(&'a self, row: &'b OwnedRow)
            -> Pin<Box<dyn Future<Output = $crate::Result<Datum>> + Send + 'async_trait>>
        where
            'a: 'async_trait,
            'b: 'async_trait,
        {
            Box::pin(async move { paste! {
                $(
                    let [<datum_ $arg:lower>] = self.[<expr_ $arg:lower>].eval_row(row).await?;
                    let [<scalar_ref_ $arg:lower>] = [<datum_ $arg:lower>].as_ref().map(|s| s.as_scalar_ref_impl().try_into().unwrap());
                )*

                let output_scalar = $macro_row!(self, $([<scalar_ref_ $arg:lower>],)*);
                let output_datum = output_scalar.map(|s| s.to_scalar_value());
                Ok(output_datum)
            }})
        }
    }
}

macro_rules! eval_normal {
    ($self:ident, $output_array:ident, $($arg:ident,)*) => {
        if let ($(Some($arg), )*) = ($($arg, )*) {
            let ret = ($self.func)($($arg, )*)?;
            let output = Some(ret.as_scalar_ref());
            $output_array.append(output);
        } else {
            $output_array.append(None);
        }
    }
}
macro_rules! eval_normal_row {
    ($self:ident, $($arg:ident,)*) => {
        if let ($(Some($arg), )*) = ($($arg, )*) {
            let ret = ($self.func)($($arg, )*)?;
            Some(ret)
        } else {
            None
        }
    }
}

macro_rules! gen_expr_normal {
    ($ty_name:ident, { $($arg:ident),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                OA: Array,
                F: Fn($($arg::RefItem<'_>, )*) -> $crate::Result<OA::OwnedItem>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )* OA)>,
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: Fn($($arg::RefItem<'_>, )*) -> $crate::Result<OA::OwnedItem> + Sync + Send,
            > fmt::Debug for $ty_name<$($arg, )* OA, F> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct(stringify!($ty_name))
                        .field("func", &std::any::type_name::<F>())
                        $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
                        .field("return_type", &self.return_type)
                        .finish()
                }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: Fn($($arg::RefItem<'_>, )*) -> $crate::Result<OA::OwnedItem> + Sync + Send,
            > Expression for $ty_name<$($arg, )* OA, F>
            where
                $(for<'a> ValueRef<'a, $arg>: std::convert::From<&'a ValueImpl>,)*
                for<'a> ValueRef<'a, OA>: std::convert::From<&'a ValueImpl>,
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { (eval_normal, eval_normal_row), $ty_name, OA, $($arg, )* }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: Fn($($arg::RefItem<'_>, )*) -> $crate::Result<OA::OwnedItem> + Sync + Send,
            > $ty_name<$($arg, )* OA, F> {
                #[allow(dead_code)]
                pub fn new(
                    $([<expr_ $arg:lower>]: BoxedExpression, )*
                    return_type: DataType,
                    func: F,
                ) -> Self {
                    Self {
                        $([<expr_ $arg:lower>], )*
                        return_type,
                        func,
                        _phantom : std::marker::PhantomData,
                    }
                }
            }
        }
    }
}

macro_rules! eval_bytes {
    ($self:ident, $output_array:ident, $($arg:ident,)*) => {
        if let ($(Some($arg), )*) = ($($arg, )*) {
            {
                let mut writer = $output_array.writer().begin();
                ($self.func)($($arg, )* &mut writer)?;
                writer.finish();
            }
        } else {
            $output_array.append(None);
        }
    }
}
macro_rules! eval_bytes_row {
    ($self:ident, $($arg:ident,)*) => {
        if let ($(Some($arg), )*) = ($($arg, )*) {
            let mut writer = String::new();
            ($self.func)($($arg, )* &mut writer)?;
            Some(Box::<str>::from(writer))
        } else {
            None
        }
    }
}

macro_rules! gen_expr_bytes {
    ($ty_name:ident, { $($arg:ident),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                F: Fn($($arg::RefItem<'_>, )* &mut dyn std::fmt::Write) -> $crate::Result<()>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )*)>,
            }

            impl<$($arg: Array, )*
                F: Fn($($arg::RefItem<'_>, )* &mut dyn std::fmt::Write) -> $crate::Result<()> + Sync + Send,
            > fmt::Debug for $ty_name<$($arg, )* F> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct(stringify!($ty_name))
                        .field("func", &std::any::type_name::<F>())
                        $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
                        .field("return_type", &self.return_type)
                        .finish()
                }
            }

            impl<$($arg: Array, )*
                F: Fn($($arg::RefItem<'_>, )* &mut dyn std::fmt::Write) -> $crate::Result<()> + Sync + Send,
            > Expression for $ty_name<$($arg, )* F>
            where
                $(
                    for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,
                    for<'a> ValueRef<'a, $arg>: std::convert::From<&'a ValueImpl>,
                )*
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { (eval_bytes, eval_bytes_row), $ty_name, Utf8Array, $($arg, )* }
            }

            impl<$($arg: Array, )*
                F: Fn($($arg::RefItem<'_>, )* &mut dyn std::fmt::Write) -> $crate::Result<()> + Sync + Send,
            > $ty_name<$($arg, )* F> {
                pub fn new(
                    $([<expr_ $arg:lower>]: BoxedExpression, )*
                    return_type: DataType,
                    func: F,
                ) -> Self {
                    Self {
                        $([<expr_ $arg:lower>], )*
                        return_type,
                        func,
                        _phantom: std::marker::PhantomData,
                    }
                }
            }
        }
    }
}

macro_rules! eval_nullable {
    ($self:ident, $output_array:ident, $($arg:ident,)*) => {
        {
            let ret = ($self.func)($($arg,)*)?;
            $output_array.append(option_as_scalar_ref(&ret));
        }
    }
}
macro_rules! eval_nullable_row {
    ($self:ident, $($arg:ident,)*) => {
        ($self.func)($($arg,)*)?
    }
}

macro_rules! gen_expr_nullable {
    ($ty_name:ident, { $($arg:ident),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                OA: Array,
                F: Fn($(Option<$arg::RefItem<'_>>, )*) -> $crate::Result<Option<OA::OwnedItem>>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )* OA)>,
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: Fn($(Option<$arg::RefItem<'_>>, )*) -> $crate::Result<Option<OA::OwnedItem>> + Sync + Send,
            > fmt::Debug for $ty_name<$($arg, )* OA, F> {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    f.debug_struct(stringify!($ty_name))
                        .field("func", &std::any::type_name::<F>())
                        $(.field(stringify!([<expr_ $arg:lower>]), &self.[<expr_ $arg:lower>]))*
                        .field("return_type", &self.return_type)
                        .finish()
                }
            }

            #[async_trait::async_trait]
            impl<$($arg: Array, )*
                OA: Array,
                F: Fn($(Option<$arg::RefItem<'_>>, )*) -> $crate::Result<Option<OA::OwnedItem>> + Sync + Send,
            > Expression for $ty_name<$($arg, )* OA, F>
            where
                $(for<'a> ValueRef<'a, $arg>: std::convert::From<&'a ValueImpl>,)*
                for<'a> ValueRef<'a, OA>: std::convert::From<&'a ValueImpl>,
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { (eval_nullable, eval_nullable_row), $ty_name, OA, $($arg, )* }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: Fn($(Option<$arg::RefItem<'_>>, )*) -> $crate::Result<Option<OA::OwnedItem>> + Sync + Send,
            > $ty_name<$($arg, )* OA, F> {
                // Compile failed due to some GAT lifetime issues so make this field private.
                // Check issues #742.
                #[allow(dead_code)]
                pub fn new(
                    $([<expr_ $arg:lower>]: BoxedExpression, )*
                    return_type: DataType,
                    func: F,
                ) -> Self {
                    Self {
                        $([<expr_ $arg:lower>], )*
                        return_type,
                        func,
                        _phantom: std::marker::PhantomData,
                    }
                }
            }
        }
    }
}

gen_expr_normal!(UnaryExpression, { IA1 });
gen_expr_normal!(BinaryExpression, { IA1, IA2 });
gen_expr_normal!(TernaryExpression, { IA1, IA2, IA3 });

gen_expr_bytes!(UnaryBytesExpression, { IA1 });
gen_expr_bytes!(BinaryBytesExpression, { IA1, IA2 });
gen_expr_bytes!(TernaryBytesExpression, { IA1, IA2, IA3 });
gen_expr_bytes!(QuaternaryBytesExpression, { IA1, IA2, IA3, IA4 });

gen_expr_nullable!(UnaryNullableExpression, { IA1 });
gen_expr_nullable!(BinaryNullableExpression, { IA1, IA2 });
