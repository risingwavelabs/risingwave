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

/// `array` builds an `Array` with `Option`.
#[macro_export]
macro_rules! array {
    ($array:ty, [$( $value:expr ),*] $(,)?) => {
        {
            use $crate::array::Array;
            use $crate::array::ArrayBuilder;
            let mut builder = <$array as Array>::Builder::new(0);
            for value in [$($value),*] {
                let value: Option<<$array as Array>::RefItem<'_>> = value.map(Into::into);
                builder.append(value);
            }
            builder.finish()
        }
    };
}

/// `empty_array` builds an empty `Array`.
#[macro_export]
macro_rules! empty_array {
    ($array:ty) => {{
        use $crate::array::{Array, ArrayBuilder};
        let builder = <$array as Array>::Builder::new(0);
        builder.finish()
    }};
}

/// `array_nonnull` builds an `Array` with concrete values.
#[macro_export]
macro_rules! array_nonnull {
    ($array:ty, [$( $value:expr ),*]) => {
        {
            use $crate::array::Array;
            use $crate::array::ArrayBuilder;
            let mut builder = <$array as Array>::Builder::new(0);
            for value in [$($value),*] {
                let value: <$array as Array>::RefItem<'_> = value.into();
                builder.append(Some(value));
            }
            builder.finish()
        }
    };
}

/// `column` builds a `Column` with `Option`.
#[macro_export]
macro_rules! column {
    ($array:ty, [$( $value:expr ),*]) => {
        {
            use $crate::array::column::Column;
            let arr = $crate::array! { $array, [ $( $value ),* ] };
            let col: Column = arr.into();
            col
        }
    };
}

/// `column_nonnull` builds a `Column` with concrete values.
#[macro_export]
macro_rules! column_nonnull {
    ($array:ty, [$( $value:expr ),*]) => {
        {
            use $crate::array::column::Column;
            let arr = $crate::array_nonnull! { $array, [ $( $value ),* ] };
            let col: Column = arr.into();
            col
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::array::{Array, I16Array};

    #[test]
    fn test_build_array() {
        let a = array! { I16Array, [Some(1i16), None, Some(3)] };
        assert_eq!(a.len(), 3);
        let a = array_nonnull! { I16Array, [1i16, 2, 3] };
        assert_eq!(a.len(), 3);
    }

    #[test]
    fn test_build_column() {
        let c = column! { I16Array, [Some(1i16), None, Some(3)] };
        assert_eq!(c.array_ref().len(), 3);
        let c = column_nonnull! { I16Array, [1i16, 2, 3] };
        assert_eq!(c.array_ref().len(), 3);
    }
}
