#[macro_export]
macro_rules! for_all_variants {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            $($x, )*
            { Int16, Int16, int16, i16, i16, I16Array, I16ArrayBuilder },
            { Int32, Int32, int32, i32, i32, I32Array, I32ArrayBuilder },
            { Int64, Int64, int64, i64, i64, I64Array, I64ArrayBuilder },
            { Int256, Int256, int256, Int256, Int256Ref<'scalar>, Int256Array, Int256ArrayBuilder },
            { Float32, Float32, float32, F32, F32, F32Array, F32ArrayBuilder },
            { Float64, Float64, float64, F64, F64, F64Array, F64ArrayBuilder },
            { Varchar, Utf8, utf8, Box<str>, &'scalar str, Utf8Array, Utf8ArrayBuilder },
            { Boolean, Bool, bool, bool, bool, BoolArray, BoolArrayBuilder },
            { Decimal, Decimal, decimal, Decimal, Decimal, DecimalArray, DecimalArrayBuilder },
            { Interval, Interval, interval, Interval, Interval, IntervalArray, IntervalArrayBuilder },
            { Date, Date, date, Date, Date, DateArray, DateArrayBuilder },
            { Time, Time, time, Time, Time, TimeArray, TimeArrayBuilder },
            { Timestamp, Timestamp, timestamp, Timestamp, Timestamp, TimestampArray, TimestampArrayBuilder },
            { Timestamptz, Timestamptz, timestamptz, Timestamptz, Timestamptz, TimestamptzArray, TimestamptzArrayBuilder },
            { Jsonb, Jsonb, jsonb, JsonbVal, JsonbRef<'scalar>, JsonbArray, JsonbArrayBuilder },
            { Serial, Serial, serial, Serial, Serial, SerialArray, SerialArrayBuilder },
            { Struct, Struct, struct, StructValue, StructRef<'scalar>, StructArray, StructArrayBuilder },
            { List, List, list, ListValue, ListRef<'scalar>, ListArray, ListArrayBuilder },
            { Bytea, Bytea, bytea, Box<[u8]>, &'scalar [u8], BytesArray, BytesArrayBuilder }
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! for_all_scalar_variants {
    ($macro:ident $(, $x:tt)*) => {
        for_all_variants! { project_scalar_variants, $macro, [ $($x, )* ] }
    };
}
#[macro_export]
macro_rules! project_scalar_variants {
    ($macro:ident, [ $($x:tt, )* ], $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        $macro! {
            $($x, )*
            $( { $variant_name, $suffix_name, $scalar, $scalar_ref } ),*
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! for_all_array_variants {
    ($macro:ident $(, $x:tt)*) => {
        for_all_variants! { project_array_variants, $macro, [ $($x, )* ] }
    };
}
#[macro_export]
macro_rules! project_array_variants {
    ($macro:ident, [ $($x:tt, )* ], $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        $macro! {
            $($x, )*
            $( { $variant_name, $suffix_name, $array, $builder } ),*
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! for_all_type_pairs {
    ($macro:ident $(, $x:tt)*) => {
        for_all_variants! { project_type_pairs, $macro, [ $($x, )* ] }
    };
}
#[macro_export]
macro_rules! project_type_pairs {
    ($macro:ident, [ $($x:tt, )* ], $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        $macro! {
            $($x, )*
            $( { $data_type, $variant_name } ),*
        }
    };
}

#[macro_export]
macro_rules! do_expand_alias {
    ($array:ty, ($(Array, $array_alias:ident,)? $(ArrayBuilder, $array_builder_alias:ident,)? $(Scalar, $scalar_alias:ident,)? $(ScalarRef, $scalar_ref_alias:ident,)?)) => {
        $(type $array_alias = $array;)?
        $(type $array_builder_alias = <$array as Array>::Builder;)?
        $(type $scalar_alias = <$array as Array>::OwnedItem;)?
        $(type $scalar_ref_alias<'scalar> = <$array as Array>::RefItem<'scalar>;)?
    };
}

#[macro_export(local_inner_macros)]
macro_rules! do_dispatch_variants {
    ($impl:expr, $type:ident, $inner:pat, [$alias:tt], $body:tt, $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        match $impl {
            $( $type::$variant_name($inner) => {
                do_expand_alias!($array, $alias);
                #[allow(unused_braces)]
                $body
            }, )*
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! dispatch_array_variants {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        dispatch_array_variants!($impl, _, [$($k = $v),*], $body)
    };
    ($impl:expr, $inner:pat, $body:tt) => {
        dispatch_array_variants!($impl, $inner, [], $body)
    };
    ($impl:expr, $inner:pat, [$($k:ident = $v:ident),*], $body:tt) => {
        for_all_variants! { do_dispatch_variants, $impl, ArrayImpl, $inner, [($($v, $k,)*)], $body }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! dispatch_array_builder_variants {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        dispatch_array_builder_variants!($impl, _, [$($k = $v),*], $body)
    };
    ($impl:expr, $inner:pat, $body:tt) => {
        dispatch_array_builder_variants!($impl, $inner, [], $body)
    };
    ($impl:expr, $inner:pat, [$($k:ident = $v:ident),*], $body:tt) => {
        for_all_variants! { do_dispatch_variants, $impl, ArrayBuilderImpl, $inner, [($($v, $k,)*)], $body }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! dispatch_scalar_variants {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        dispatch_scalar_variants!($impl, _, [$($k = $v),*], $body)
    };
    ($impl:expr, $inner:pat, $body:tt) => {
        dispatch_scalar_variants!($impl, $inner, [], $body)
    };
    ($impl:expr, $inner:pat, [$($k:ident = $v:ident),*], $body:tt) => {
        for_all_variants! { do_dispatch_variants, $impl, ScalarImpl, $inner, [($($v, $k,)*)], $body }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! do_dispatch_data_types {
    ($impl:expr, [$alias:tt], $body:tt, $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        match $impl {
            $( $crate::types::DataType::$data_type { .. } => {
                do_expand_alias!($array, $alias);
                #[allow(unused_braces)]
                $body
            }, )*
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! dispatch_data_types {
    ($impl:expr, [$($k:ident = $v:ident),*], $body:tt) => {
        for_all_variants! { do_dispatch_data_types, $impl, [($($v, $k,)*)], $body }
    };
}
