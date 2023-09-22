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
    ($($ret:tt).+  $func_name:ident($($args:tt)*)) => {
        concat! {"(", gen_jni_sig_inner!{$($args)*}, ")", gen_jni_sig_inner! {$($ret).+} }
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
                static native long hummockIteratorNew(byte[] readPlan);
                static native long hummockIteratorNext(long pointer);
                static native void hummockIteratorClose(long pointer);
                static native byte.[] rowGetKey(long pointer);
                static native int rowGetOp(long pointer);
                static native boolean rowIsNull(long pointer, int index);
                static native short rowGetInt16Value(long pointer, int index);
                static native int rowGetInt32Value(long pointer, int index);
                static native long rowGetInt64Value(long pointer, int index);
                static native float rowGetFloatValue(long pointer, int index);
                static native double rowGetDoubleValue(long pointer, int index);
                static native boolean rowGetBooleanValue(long pointer, int index);
                static native String rowGetStringValue(long pointer, int index);
                static native java.sql.Timestamp rowGetTimestampValue(long pointer, int index);
                static native java.math.BigDecimal rowGetDecimalValue(long pointer, int index);
                static native java.sql.Time rowGetTimeValue(long pointer, int index);
                static native java.sql.Date rowGetDateValue(long pointer, int index);
                static native String rowGetIntervalValue(long pointer, int index);
                static native String rowGetJsonbValue(long pointer, int index);
                static native byte.[] rowGetByteaValue(long pointer, int index);
                static native Object rowGetArrayValue(long pointer, int index, Class clazz);
                static native void rowClose(long pointer);
                static native long streamChunkIteratorNew(byte[] streamChunkPayload);
                static native long streamChunkIteratorNext(long pointer);
                static native void streamChunkIteratorClose(long pointer);
                static native long streamChunkIteratorFromPretty(String str);
                public static native boolean sendCdcSourceMsgToChannel(long channelPtr, byte[] msg);
            }
            $(,$args)*
        }
    };
}

#[macro_export]
macro_rules! for_all_native_methods {
    (
        {
            $($(public)? static native $($ret:tt).+  $func_name:ident($($args:tt)*);)*
        },
        $macro:path
        $(,$extra_args:tt)*
    ) => {
        $macro! {
            {
                $(
                    { $func_name, {gen_jni_sig! {$($ret).+ $func_name($($args)*)}}}
                ),*
            }
            $(,$extra_args)*
        }
    };
    ($macro:path $(,$args:tt)*) => {
        $crate::for_all_plain_native_methods! {
            $crate::for_all_native_methods,
            $macro
            $(,$args)*
        }
    };
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
        assert_eq!(gen_jni_sig!(byte.[] rowGetKey(long pointer)), "(J)[B");
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
                        public static native byte.[] rowGetKey(long pointer);
                    },
                    gen_array
                }
            }};
            (all) => {{
                for_all_native_methods! {
                    gen_array
                }
            }};
            ({$({ $func_name:ident, $sig:expr }),*}) => {{
                [
                    $(
                        (stringify! {$func_name}, $sig),
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
