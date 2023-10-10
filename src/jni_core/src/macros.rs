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

#[macro_export]
macro_rules! gen_class_name {
    ($last:ident) => {
        stringify! {$last}
    };
    ($first:ident . $($rest:ident).+) => {
        concat! {stringify! {$first}, "/", gen_class_name! {$($rest).+} }
    }
}

#[macro_export]
macro_rules! gen_jni_sig_inner {
    ($(public)? static native $($rest:tt)*) => {
        gen_jni_sig_inner! { $($rest)* }
    };
    ($($ret:ident).+  $($func_name:ident)? ($($args:tt)*)) => {
        concat! {"(", gen_jni_sig_inner!{$($args)*}, ")", gen_jni_sig_inner! {$($ret).+} }
    };
    ($($ret:ident).+ []  $($func_name:ident)? ($($args:tt)*)) => {
        concat! {"(", gen_jni_sig_inner!{$($args)*}, ")", gen_jni_sig_inner! {$($ret).+ []} }
    };
    (boolean) => {
        "Z"
    };
    (byte) => {
        "B"
    };
    (char) => {
        "C"
    };
    (short) => {
        "S"
    };
    (int) => {
        "I"
    };
    (long) => {
        "J"
    };
    (float) => {
        "F"
    };
    (double) => {
        "D"
    };
    (void) => {
        "V"
    };
    (String) => {
        gen_jni_sig_inner! { java.lang.String }
    };
    (Object) => {
        gen_jni_sig_inner! { java.lang.Object }
    };
    (Class) => {
        gen_jni_sig_inner! { java.lang.Class }
    };
    ($($class_part:ident).+) => {
        concat! {"L", gen_class_name! {$($class_part).+}, ";"}
    };
    ($($class_part:ident).+ $(.)? [] $($param_name:ident)? $(,$($rest:tt)*)?) => {
        concat! { "[", gen_jni_sig_inner! {$($class_part).+}, gen_jni_sig_inner! {$($($rest)*)?}}
    };
    (Class $(< ? >)? $($param_name:ident)? $(,$($rest:tt)*)?) => {
        concat! { gen_jni_sig_inner! { Class }, gen_jni_sig_inner! {$($($rest)*)?}}
    };
    ($($class_part:ident).+ $($param_name:ident)? $(,$($rest:tt)*)?) => {
        concat! { gen_jni_sig_inner! {$($class_part).+}, gen_jni_sig_inner! {$($($rest)*)?}}
    };
    () => {
        ""
    };
    ($($invalid:tt)*) => {
        compile_error!(concat!("unsupported type {{", stringify!($($invalid)*), "}}"))
    };
}

#[macro_export]
macro_rules! gen_jni_sig {
    ($($input:tt)*) => {{
        // this macro only provide with a expression context
        gen_jni_sig_inner! {$($input)*}
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

                public static native byte[] recvSinkCoordinatorRequestFromChannel(long channelPtr);

                public static native boolean sendSinkCoordinatorResponseToChannel(long channelPtr, byte[] msg);
            }
            $(,$args)*
        }
    };
}

#[macro_export]
macro_rules! for_single_native_method {
    (
        {$($ret:tt).+ $func_name:ident ($($args:tt)*)},
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $macro! {
            $func_name,
            {$($ret).+},
            {$($args)*}
        }
    };
    (
        {$($ret:tt).+ [] $func_name:ident ($($args:tt)*)},
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $macro! {
            $func_name,
            {$($ret).+ []},
            {$($args)*}
        }
    };
}

#[macro_export]
macro_rules! for_all_native_methods {
    (
        {$($input:tt)*},
        $macro:path
        $(,$extra_args:tt)*
    ) => {{
        $crate::for_all_native_methods! {
            {$($input)*},
            {},
            $macro
            $(,$extra_args)*
        }
    }};
    (
        {
            $(public)? static native $($ret:tt).+ $func_name:ident($($args:tt)*); $($rest:tt)*
        },
        {
            $({$prev_func_name:ident, {$($prev_ret:tt)*}, {$($prev_args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::for_all_native_methods! {
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
            $(public)? static native $($ret:tt).+ [] $func_name:ident($($args:tt)*); $($rest:tt)*
        },
        {
            $({$prev_func_name:ident, {$($prev_ret:tt)*}, {$($prev_args:tt)*}})*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $crate::for_all_native_methods! {
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
                $({$func_name, {$($ret)*}, {$($args)*}}),*
            }
            $(,$extra_args)*
        }
    };
    ($macro:path $(,$args:tt)*) => {{
        $crate::for_all_plain_native_methods! {
            $crate::for_all_native_methods,
            $macro
            $(,$args)*
        }
    }};
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_gen_jni_sig() {
        assert_eq!(gen_jni_sig!(int), "I");
        assert_eq!(gen_jni_sig!(boolean f(int, short, byte[])), "(IS[B)Z");
        assert_eq!(
            gen_jni_sig!(boolean f(int, short, byte[], java.lang.String)),
            "(IS[BLjava/lang/String;)Z"
        );
        assert_eq!(
            gen_jni_sig!(boolean f(int, java.lang.String)),
            "(ILjava/lang/String;)Z"
        );
        assert_eq!(gen_jni_sig!(public static native int vnodeCount()), "()I");
        assert_eq!(
            gen_jni_sig!(long hummockIteratorNew(byte[] readPlan)),
            "([B)J"
        );
        assert_eq!(gen_jni_sig!(long hummockIteratorNext(long pointer)), "(J)J");
        assert_eq!(
            gen_jni_sig!(void hummockIteratorClose(long pointer)),
            "(J)V"
        );
        assert_eq!(gen_jni_sig!(byte[] rowGetKey(long pointer)), "(J)[B");
        assert_eq!(
            gen_jni_sig!(java.sql.Timestamp rowGetTimestampValue(long pointer, int index)),
            "(JI)Ljava/sql/Timestamp;"
        );
        assert_eq!(
            gen_jni_sig!(String rowGetStringValue(long pointer, int index)),
            "(JI)Ljava/lang/String;"
        );
        assert_eq!(
            gen_jni_sig!(static native Object rowGetArrayValue(long pointer, int index, Class clazz)),
            "(JILjava/lang/Class;)Ljava/lang/Object;"
        );
    }

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
            ({$({ $func_name:ident, {$($ret:tt)+}, {$($args:tt)*} }),*}) => {{
                [
                    $(
                        (stringify! {$func_name}, gen_jni_sig! { $($ret)+ ($($args)*)}),
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
}
