// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//! Template macro to generate code for unary/binary/ternary expression.

use std::fmt;
use std::sync::Arc;

use itertools::{multizip, Itertools};
use paste::paste;

use crate::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayRef, BytesGuard, BytesWriter, DataChunk, Utf8Array,
};
use crate::error::Result;
use crate::expr::{BoxedExpression, Expression};
use crate::types::{option_as_scalar_ref, DataType, Scalar};

macro_rules! gen_eval {
    { $macro:tt, $ty_name:ident, $OA:ty, $($arg:ident,)* } => {
        fn eval(&self, data_chunk: &DataChunk) -> Result<ArrayRef> {
            paste! {
                $(
                    let [<ret_ $arg:lower>] = self.[<expr_ $arg:lower>].eval(data_chunk)?;
                    let [<arr_ $arg:lower>]: &$arg = [<ret_ $arg:lower>].as_ref().into();
                )*

                let bitmap = data_chunk.get_visibility_ref();
                let mut output_array = <$OA as Array>::Builder::new(data_chunk.capacity())?;
                Ok(Arc::new(match bitmap {
                    Some(bitmap) => {
                        for (($([<v_ $arg:lower>], )*), visible) in multizip(($([<arr_ $arg:lower>].iter(), )*)).zip_eq(bitmap.iter()) {
                            if !visible {
                                continue;
                            }
                            $macro!(self, output_array, $([<v_ $arg:lower>],)*)
                        }
                        output_array.finish()?.into()
                    }
                    None => {
                        for ($([<v_ $arg:lower>], )*) in multizip(($([<arr_ $arg:lower>].iter(), )*)) {
                            $macro!(self, output_array, $([<v_ $arg:lower>],)*)
                        }
                        output_array.finish()?.into()
                    }
                }))
            }
        }
    }
}

macro_rules! eval_normal {
    ($self:ident, $output_array:ident, $($arg:ident,)*) => {
        if let ($(Some($arg), )*) = ($($arg, )*) {
            let ret = ($self.func)($($arg, )*)?;
            let output = Some(ret.as_scalar_ref());
            $output_array.append(output)?;
        } else {
            $output_array.append(None)?;
        }
    }
}

macro_rules! gen_expr_normal {
    ($ty_name:ident, { $($arg:ident),* }, { $($lt:lifetime),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> Result<OA::OwnedItem>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )* OA)>,
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
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
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
            >    Expression for $ty_name<$($arg, )* OA, F>
            where
                $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
                for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { eval_normal, $ty_name, OA, $($arg, )* }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )*) -> Result<OA::OwnedItem> + Sized + Sync + Send,
            > $ty_name<$($arg, )* OA, F> {
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
            let writer = $output_array.writer();
            let guard = ($self.func)($($arg, )* writer)?;
            $output_array = guard.into_inner();
        } else {
            $output_array.append(None)?;
        }
    }
}

macro_rules! gen_expr_bytes {
    ($ty_name:ident, { $($arg:ident),* }, { $($lt:lifetime),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> Result<BytesGuard>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )*)>,
            }

            impl<$($arg: Array, )*
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> Result<BytesGuard> + Sized + Sync + Send,
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
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> Result<BytesGuard> + Sized + Sync + Send,
            > Expression for $ty_name<$($arg, )* F>
            where
                $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { eval_bytes, $ty_name, Utf8Array, $($arg, )* }
            }

            impl<$($arg: Array, )*
                F: for<$($lt),*> Fn($($arg::RefItem<$lt>, )* BytesWriter) -> Result<BytesGuard> + Sized + Sync + Send,
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
            $output_array.append(option_as_scalar_ref(&ret))?;
        }
    }
}

macro_rules! gen_expr_nullable {
    ($ty_name:ident, { $($arg:ident),* }, { $($lt:lifetime),* }) => {
        paste! {
            pub struct $ty_name<
                $($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> Result<Option<OA::OwnedItem>>,
            > {
                $([<expr_ $arg:lower>]: BoxedExpression,)*
                return_type: DataType,
                func: F,
                _phantom: std::marker::PhantomData<($($arg, )* OA)>,
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
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
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
            > Expression for $ty_name<$($arg, )* OA, F>
            where
                $(for<'a> &'a $arg: std::convert::From<&'a ArrayImpl>,)*
                for<'a> &'a OA: std::convert::From<&'a ArrayImpl>,
            {
                fn return_type(&self) -> DataType {
                    self.return_type.clone()
                }

                gen_eval! { eval_nullable, $ty_name, OA, $($arg, )* }
            }

            impl<$($arg: Array, )*
                OA: Array,
                F: for<$($lt),*> Fn($(Option<$arg::RefItem<$lt>>, )*) -> Result<Option<OA::OwnedItem>> + Sized + Sync + Send,
            > $ty_name<$($arg, )* OA, F> {
                // Compile failed due to some GAT lifetime issues so make this field private.
                // Check issues #742.
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

gen_expr_normal!(UnaryExpression, { IA1 }, { 'ia1 });
gen_expr_normal!(BinaryExpression, { IA1, IA2 }, { 'ia1, 'ia2 });
gen_expr_normal!(TernaryExpression, { IA1, IA2, IA3 }, { 'ia1, 'ia2, 'ia3 });

gen_expr_bytes!(UnaryBytesExpression, { IA1 }, { 'ia1 });
gen_expr_bytes!(BinaryBytesExpression, { IA1, IA2 }, { 'ia1, 'ia2 });
gen_expr_bytes!(TernaryBytesExpression, { IA1, IA2, IA3 }, { 'ia1, 'ia2, 'ia3 });

gen_expr_nullable!(UnaryNullableExpression, { IA1 }, { 'ia1 });
gen_expr_nullable!(BinaryNullableExpression, { IA1, IA2 }, { 'ia1, 'ia2 });
