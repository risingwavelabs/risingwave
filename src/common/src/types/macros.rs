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
macro_rules! for_all_scalar_variants {
    ($macro:ident $(, $x:tt)*) => {
        for_all_variants! { project_scalar_variants, $macro, [ $($x, )* ] }
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
macro_rules! for_all_array_variants {
    ($macro:ident $(, $x:tt)*) => {
        for_all_variants! { project_array_variants, $macro, [ $($x, )* ] }
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

#[macro_export(local_inner_macros)]
macro_rules! for_all_type_pairs {
    ($macro:ident $(, $x:tt)*) => {
        for_all_variants! { project_type_pairs, $macro, [ $($x, )* ] }
    };
}

#[macro_export]
macro_rules! do_dispatch {
    ($impl:expr, $type:ident, $inner:pat, $body:tt, $( { $_:ident, $variant_name:ident } ),*) => {
        match $impl {
            $( $type::$variant_name($inner) => $body, )*
        }
    };
}

#[macro_export(local_inner_macros)]
macro_rules! dispatch_all_variants {
    ($impl:expr,array: $inner:pat, $body:tt) => {{
        for_all_type_pairs! { do_dispatch, $impl, ArrayImpl, $inner, $body }
    }};
    ($impl:expr,array_builder: $inner:pat, $body:tt) => {{
        for_all_type_pairs! { do_dispatch, $impl, ArrayBuilderImpl, $inner, $body }
    }};
    ($impl:expr,scalar: $inner:pat, $body:tt) => {{
        for_all_type_pairs! { do_dispatch, $impl, ScalarImpl, $inner, $body }
    }};
}

#[macro_export]
macro_rules! do_data_type_dispatch {
    ($impl:expr, $array_alias:ident, $body:tt, $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        match $impl {
            $( DataType::$data_type { .. } => {
                type $array_alias = $array;
                $body
            }, )*
        }
    };
}

// TODO:
#[macro_export(local_inner_macros)]
macro_rules! dispatch_data_types {
    ($impl:expr, $array_alias:ident, $body:tt) => {{
        for_all_variants! { do_data_type_dispatch, $impl, $array_alias, $body }
    }};
}

#[macro_export]
macro_rules! do_phys_type_dispatch {
    ($type:ident, $impl:expr, $array_alias:ident, $body:tt, $( { $data_type:ident, $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty, $array:ty, $builder:ty } ),*) => {
        match $impl {
            $( $type::$variant_name { .. } => {
                type $array_alias = $array;
                $body
            }, )*
        }
    };
}

// TODO:
#[macro_export(local_inner_macros)]
macro_rules! dispatch_phys_types {
    ($type:ident, $impl:expr, $array_alias:ident, $body:tt) => {{
        for_all_variants! { do_phys_type_dispatch, $type, $impl, $array_alias, $body }
    }};
}
