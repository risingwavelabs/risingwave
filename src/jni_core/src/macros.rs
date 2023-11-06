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

// Utils
/// A macro that splits the input by comma and calls the callback `$macro` with the split result.
/// The each part of the split result is within a bracket, and the callback `$macro` can have its
/// first parameter as `{$({$($args:tt)+})*}` to match the split result.
///
/// ```
/// macro_rules! call_split_by_comma {
///     ({$({$($args:tt)+})*}) => {
///         [$(
///            stringify! {$($args)+}
///         ),*]
///     };
///     ($($input:tt)*) => {{
///         risingwave_jni_core::split_by_comma! {
///             {$($input)*},
///             call_split_by_comma
///         }
///     }};
/// }
///
/// let expected_result = [
///     "hello",
///     "my friend",
/// ];
///
/// assert_eq!(expected_result, {call_split_by_comma!(hello, my friend)});
/// ```
#[macro_export]
macro_rules! split_by_comma {
    // Entry of macro. Put input as the first parameter and initialize empty
    // second and third parameter.
    (
        {$($input:tt)*},
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {$($input)*},
            {}, // previous splits
            {}, // current split
            $macro $(,$args)*
        }
    };
    // When the first token is comma, move the tokens of current split to the
    // list of previous splits with a bracket wrapped, and then clear the current
    // split.
    (
        {
            ,
            $($rest:tt)*
        },
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {
            $($current_split:tt)*
        },
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {
                $($rest)*
            },
            {
                $(
                    {$($prev_split)*}
                )*
                {
                    $($current_split)*
                }
            },
            {},
            $macro $(,$args)*
        }
    };
    // Do the same as the previous match when we just reach the end of input,
    // with the content of last split not added to the list of previous split yet.
    (
        {},
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {
            $($current_split:tt)+
        },
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {},
            {
                $(
                    {$($prev_split)*}
                )*
                {
                    $($current_split)+
                }
            },
            {},
            $macro $(,$args)*
        }
    };
    // For a token that is not comma, add to the list of current staging split.
    (
        {
            $first:tt
            $($rest:tt)*
        },
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {
            $($current_split:tt)*
        },
        $macro:path $(,$args:tt)*
    ) => {
        $crate::split_by_comma! {
            {
                $($rest)*
            },
            {
                $(
                    {$($prev_split)*}
                )*
            },
            {
                $($current_split)* $first
            },
            $macro $(,$args)*
        }
    };
    // When all split result are added to the list, call the callback `$macro` with the result.
    (
        {},
        {
            $(
                {$($prev_split:tt)*}
            )*
        },
        {},
        $macro:path $(,$args:tt)*
    ) => {
        $macro! {
            {
                $(
                    {$($prev_split)*}
                )*
            }
            $(,$args)*
        }
    };
}
// End of utils part

/// Generate the dot separated java class name to the slash separated name.
///
/// ```
/// assert_eq!(
///     "java/lang/String",
///     risingwave_jni_core::gen_class_name!(java.lang.String)
/// )
/// ```
#[macro_export]
macro_rules! gen_class_name {
    ($last:ident) => {
        stringify! {$last}
    };
    ($first:ident . $($rest:ident).+) => {
        concat! {stringify! {$first}, "/", $crate::gen_class_name! {$($rest).+} }
    }
}

/// Generate the type signature of a single type
/// ```
/// use risingwave_jni_core::gen_jni_type_sig;
/// assert_eq!("Z", gen_jni_type_sig!(boolean));
/// assert_eq!("B", gen_jni_type_sig!(byte));
/// assert_eq!("C", gen_jni_type_sig!(char));
/// assert_eq!("S", gen_jni_type_sig!(short));
/// assert_eq!("I", gen_jni_type_sig!(int));
/// assert_eq!("J", gen_jni_type_sig!(long));
/// assert_eq!("F", gen_jni_type_sig!(float));
/// assert_eq!("D", gen_jni_type_sig!(double));
/// assert_eq!("V", gen_jni_type_sig!(void));
/// assert_eq!("Ljava/lang/Class;", gen_jni_type_sig!(Class<?>));
/// assert_eq!("Ljava/lang/String;", gen_jni_type_sig!(String));
/// assert_eq!("[B", gen_jni_type_sig!(byte[]));
/// ```
#[macro_export]
macro_rules! gen_jni_type_sig {
    (boolean $($param_name:ident)?) => {
        "Z"
    };
    (byte $($param_name:ident)?) => {
        "B"
    };
    (char $($param_name:ident)?) => {
        "C"
    };
    (short $($param_name:ident)?) => {
        "S"
    };
    (int $($param_name:ident)?) => {
        "I"
    };
    (long $($param_name:ident)?) => {
        "J"
    };
    (float $($param_name:ident)?) => {
        "F"
    };
    (double $($param_name:ident)?) => {
        "D"
    };
    (void) => {
        "V"
    };
    (String $($param_name:ident)?) => {
        $crate::gen_jni_type_sig! { java.lang.String }
    };
    (Object $($param_name:ident)?) => {
        $crate::gen_jni_type_sig! { java.lang.Object }
    };
    (Class $(< ? >)? $($param_name:ident)?) => {
        $crate::gen_jni_type_sig! { java.lang.Class }
    };
    ($($class_part:ident).+ $($param_name:ident)?) => {
        concat! {"L", $crate::gen_class_name! {$($class_part).+}, ";"}
    };
    ($($class_part:ident).+ [] $($param_name:ident)?) => {
        concat! { "[", $crate::gen_jni_type_sig! {$($class_part).+}}
    };
    ($($invalid:tt)*) => {
        compile_error!(concat!("unsupported type `", stringify!($($invalid)*), "`"))
    };
}

/// Generate the jni signature of a given function
/// ```
/// #![feature(lazy_cell)]
/// use risingwave_jni_core::gen_jni_sig;
/// assert_eq!(gen_jni_sig!(boolean f(int, short, byte[])), "(IS[B)Z");
/// assert_eq!(
///     gen_jni_sig!(boolean f(int, short, byte[], java.lang.String)),
///     "(IS[BLjava/lang/String;)Z"
/// );
/// assert_eq!(
///     gen_jni_sig!(boolean f(int, java.lang.String)),
///     "(ILjava/lang/String;)Z"
/// );
/// assert_eq!(gen_jni_sig!(public static native int vnodeCount()), "()I");
/// assert_eq!(
///     gen_jni_sig!(long hummockIteratorNew(byte[] readPlan)),
///     "([B)J"
/// );
/// assert_eq!(gen_jni_sig!(long hummockIteratorNext(long pointer)), "(J)J");
/// assert_eq!(
///     gen_jni_sig!(void hummockIteratorClose(long pointer)),
///     "(J)V"
/// );
/// assert_eq!(gen_jni_sig!(byte[] rowGetKey(long pointer)), "(J)[B");
/// assert_eq!(
///     gen_jni_sig!(java.sql.Timestamp rowGetTimestampValue(long pointer, int index)),
///     "(JI)Ljava/sql/Timestamp;"
/// );
/// assert_eq!(
///     gen_jni_sig!(String rowGetStringValue(long pointer, int index)),
///     "(JI)Ljava/lang/String;"
/// );
/// assert_eq!(
///     gen_jni_sig!(static native Object rowGetArrayValue(long pointer, int index, Class clazz)),
///     "(JILjava/lang/Class;)Ljava/lang/Object;"
/// );
/// ```
#[macro_export]
macro_rules! gen_jni_sig {
    // handle the result of `split_by_comma`
    ({$({$($args:tt)+})*}, {return {$($ret:tt)*}}) => {{
        concat! {
            "(", $($crate::gen_jni_type_sig!{ $($args)+ },)* ")",
            $crate::gen_jni_type_sig! {$($ret)+}
        }
    }};
    ({$($ret:tt)*}, {$($args:tt)*}) => {{
        $crate::split_by_comma! {
            {$($args)*},
            $crate::gen_jni_sig,
            {return {$($ret)*}}
        }
    }};
    // handle the result of `split_extract_plain_native_methods`
    ({{$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}}}) => {{
        $crate::gen_jni_sig! {
            {$($ret)*}, {$($args)*}
        }
    }};
    ($($input:tt)*) => {{
        $crate::split_extract_plain_native_methods! {{$($input)*;}, $crate::gen_jni_sig}
    }}
}

#[macro_export]
macro_rules! for_all_plain_native_methods {
    ($macro:path $(,$args:tt)*) => {
        $macro! {
            {
                public static native int vnodeCount();

                // hummock iterator method
                // Return a pointer to the iterator
                static native long iteratorNewHummock(byte[] readPlan);

                static native boolean iteratorNext(long pointer);

                static native void iteratorClose(long pointer);

                static native long iteratorNewFromStreamChunkPayload(byte[] streamChunkPayload);

                static native long iteratorNewFromStreamChunkPretty(String str);

                static native byte[] iteratorGetKey(long pointer);

                static native int iteratorGetOp(long pointer);

                static native boolean iteratorIsNull(long pointer, int index);

                static native short iteratorGetInt16Value(long pointer, int index);

                static native int iteratorGetInt32Value(long pointer, int index);

                static native long iteratorGetInt64Value(long pointer, int index);

                static native float iteratorGetFloatValue(long pointer, int index);

                static native double iteratorGetDoubleValue(long pointer, int index);

                static native boolean iteratorGetBooleanValue(long pointer, int index);

                static native String iteratorGetStringValue(long pointer, int index);

                static native java.sql.Timestamp iteratorGetTimestampValue(long pointer, int index);

                static native java.math.BigDecimal iteratorGetDecimalValue(long pointer, int index);

                static native java.sql.Time iteratorGetTimeValue(long pointer, int index);

                static native java.sql.Date iteratorGetDateValue(long pointer, int index);

                static native String iteratorGetIntervalValue(long pointer, int index);

                static native String iteratorGetJsonbValue(long pointer, int index);

                static native byte[] iteratorGetByteaValue(long pointer, int index);

                // TODO: object or object array?
                static native Object iteratorGetArrayValue(long pointer, int index, Class<?> clazz);

                public static native boolean sendCdcSourceMsgToChannel(long channelPtr, byte[] msg);

                public static native byte[] recvSinkWriterRequestFromChannel(long channelPtr);

                public static native boolean sendSinkWriterResponseToChannel(long channelPtr, byte[] msg);

                public static native boolean sendSinkWriterErrorToChannel(long channelPtr, String msg);

                public static native byte[] recvSinkCoordinatorRequestFromChannel(long channelPtr);

                public static native boolean sendSinkCoordinatorResponseToChannel(long channelPtr, byte[] msg);
            }
            $(,$args)*
        }
    };
}

/// Given the plain text of a list native methods, split the methods by semicolon (;), extract
/// the return type, argument list and name of the methods and pass the result to the callback
/// `$macro` with the extracted result as the first parameter. The result can be matched with
/// pattern `{$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}`
///
/// ```
/// macro_rules! call_split_extract_plain_native_methods {
///     ({$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}) => {
///         [$(
///             (stringify! {$func_name}, stringify!{$($ret)*}, stringify!{($($args)*)})
///         ),*]
///     };
///     ($($input:tt)*) => {{
///         risingwave_jni_core::split_extract_plain_native_methods! {
///             {$($input)*},
///             call_split_extract_plain_native_methods
///         }
///     }}
/// }
/// assert_eq!([
///     ("f", "int", "(int param1, boolean param2)"),
///     ("f2", "boolean []", "()"),
///     ("f3", "java.lang.String", "(byte [] param)")
/// ], call_split_extract_plain_native_methods!(
///     int f(int param1, boolean param2);
///     boolean[] f2();
///     java.lang.String f3(byte[] param);
/// ))
/// ```
#[macro_export]
macro_rules! split_extract_plain_native_methods {
    (
        {$($input:tt)*},
        $macro:path
        $(,$extra_args:tt)*
    ) => {{
        $crate::split_extract_plain_native_methods! {
            {$($input)*},
            {},
            $macro
            $(,$extra_args)*
        }
    }};
    (
        {
            $(public)? static native $($first:tt)*
        },
        {
            $($second:tt)*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::split_extract_plain_native_methods! {
            {
                $($first)*
            },
            {
                $($second)*
            },
            $macro
            $(,$extra_args)*
        }
    };
    (
        {
            $($ret:tt).+ $func_name:ident($($args:tt)*); $($rest:tt)*
        },
        {
            $({$prev_func_name:ident, {$($prev_ret:tt)*}, {$($prev_args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::split_extract_plain_native_methods! {
            {$($rest)*},
            {
                $({$prev_func_name, {$($prev_ret)*}, {$($prev_args)*}})*
                {$func_name, {$($ret).+}, {$($args)*}}
            },
            $macro
            $(,$extra_args)*
        }
    };
    (
        {
            $($ret:tt).+ [] $func_name:ident($($args:tt)*); $($rest:tt)*
        },
        {
            $({$prev_func_name:ident, {$($prev_ret:tt)*}, {$($prev_args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::split_extract_plain_native_methods! {
            {$($rest)*},
            {
                $({$prev_func_name, {$($prev_ret)*}, {$($prev_args)*}})*
                {$func_name, {$($ret).+ []}, {$($args)*}}
            },
            $macro
            $(,$extra_args)*
        }
    };
    (
        {},
        {
            $({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $macro! {
            {
                $({$func_name, {$($ret)*}, {$($args)*}})*
            }
            $(,$extra_args)*
        }
    };
    ($($invalid:tt)*) => {
        compile_error!(concat!("unable to split extract `", stringify!($($invalid)*), "`"))
    };
}

/// Pass the information of all native methods to the callback `$macro`. The input can be matched
/// with pattern `{$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}`
#[macro_export]
macro_rules! for_all_native_methods {
    ($macro:path $(,$args:tt)*) => {{
        $crate::for_all_plain_native_methods! {
            $crate::for_all_native_methods,
            $macro
            $(,$args)*
        }
    }};
    (
        {
            $({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {{
        $macro! {
            {$({$func_name, {$($ret)*}, {$($args)*}})*}
            $(,$extra_args)*
        }
    }};
    (
        {$($input:tt)*},
        $macro:path
        $(,$extra_args:tt)*
    ) => {{
        $crate::split_extract_plain_native_methods! {
            {$($input)*},
            $crate::for_all_native_methods,
            $macro
            $(,$extra_args)*
        }
    }};
}

#[cfg(test)]
mod tests {
    use std::fmt::Formatter;

    #[test]
    fn test_for_all_gen() {
        macro_rules! gen_array {
            (test) => {{
                for_all_native_methods! {
                    {
                        public static native int vnodeCount();
                        static native long hummockIteratorNew(byte[] readPlan);
                        public static native byte[] rowGetKey(long pointer);
                    },
                    gen_array
                }
            }};
            (all) => {{
                for_all_native_methods! {
                    gen_array
                }
            }};
            ({$({ $func_name:ident, {$($ret:tt)+}, {$($args:tt)*} })*}) => {{
                [
                    $(
                        (stringify! {$func_name}, gen_jni_sig! { {$($ret)+}, {$($args)*}}),
                    )*
                ]
            }};
        }
        let sig: [(_, _); 3] = gen_array!(test);
        assert_eq!(
            sig,
            [
                ("vnodeCount", "()I"),
                ("hummockIteratorNew", "([B)J"),
                ("rowGetKey", "(J)[B")
            ]
        );

        let sig = gen_array!(all);
        assert!(!sig.is_empty());
    }

    #[test]
    fn test_all_native_methods() {
        // This test shows the signature of all native methods
        let expected = expect_test::expect![[r#"
            [
                vnodeCount                               ()I,
                iteratorNewHummock                       ([B)J,
                iteratorNext                             (J)Z,
                iteratorClose                            (J)V,
                iteratorNewFromStreamChunkPayload        ([B)J,
                iteratorNewFromStreamChunkPretty         (Ljava/lang/String;)J,
                iteratorGetKey                           (J)[B,
                iteratorGetOp                            (J)I,
                iteratorIsNull                           (JI)Z,
                iteratorGetInt16Value                    (JI)S,
                iteratorGetInt32Value                    (JI)I,
                iteratorGetInt64Value                    (JI)J,
                iteratorGetFloatValue                    (JI)F,
                iteratorGetDoubleValue                   (JI)D,
                iteratorGetBooleanValue                  (JI)Z,
                iteratorGetStringValue                   (JI)Ljava/lang/String;,
                iteratorGetTimestampValue                (JI)Ljava/sql/Timestamp;,
                iteratorGetDecimalValue                  (JI)Ljava/math/BigDecimal;,
                iteratorGetTimeValue                     (JI)Ljava/sql/Time;,
                iteratorGetDateValue                     (JI)Ljava/sql/Date;,
                iteratorGetIntervalValue                 (JI)Ljava/lang/String;,
                iteratorGetJsonbValue                    (JI)Ljava/lang/String;,
                iteratorGetByteaValue                    (JI)[B,
                iteratorGetArrayValue                    (JILjava/lang/Class;)Ljava/lang/Object;,
                sendCdcSourceMsgToChannel                (J[B)Z,
                recvSinkWriterRequestFromChannel         (J)[B,
                sendSinkWriterResponseToChannel          (J[B)Z,
                sendSinkWriterErrorToChannel             (JLjava/lang/String;)Z,
                recvSinkCoordinatorRequestFromChannel    (J)[B,
                sendSinkCoordinatorResponseToChannel     (J[B)Z,
            ]
        "#]];

        struct MethodInfo {
            name: &'static str,
            sig: &'static str,
        }

        impl std::fmt::Debug for MethodInfo {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:40} {}", self.name, self.sig)
            }
        }

        macro_rules! gen_all_native_method_info {
            () => {{
                $crate::for_all_native_methods! {
                    gen_all_native_method_info
                }
            }};
            ({$({$func_name:ident, {$($ret:tt)*}, {$($args:tt)*}})*}) => {
                [$(
                    (
                        MethodInfo {
                            name: stringify! {$func_name},
                            sig: $crate::gen_jni_sig! {
                                {$($ret)*}, {$($args)*}
                            },
                        }
                    )
                ),*]
            }
        }
        let info = gen_all_native_method_info!();
        expected.assert_debug_eq(&info);
    }
}
