// Copyright 2025 RisingWave Labs
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

use risingwave_common::array::ListRef;
use risingwave_common::types::ScalarRefImpl;
use risingwave_expr::function;

#[function("array_access(anyarray, int4) -> any")]
fn array_access(list: ListRef<'_>, index: i32) -> Option<ScalarRefImpl<'_>> {
    // index must be greater than 0 following a one-based numbering convention for arrays
    if index < 1 {
        return None;
    }
    // returns `NULL` if index is out of bounds
    list.get(index as usize - 1).flatten()
}

#[cfg(test)]
mod tests {

    use risingwave_common::array::ListValue;
    use risingwave_common::types::{DataType, Scalar};

    use super::*;

    #[test]
    fn test_int4_array_access() {
        let v1 = ListValue::from_iter([1, 2, 3]);
        let l1 = v1.as_scalar_ref();

        assert_eq!(array_access(l1, 1), Some(1.into()));
        assert_eq!(array_access(l1, -1), None);
        assert_eq!(array_access(l1, 0), None);
        assert_eq!(array_access(l1, 4), None);
    }

    #[test]
    fn test_utf8_array_access() {
        let v1 = ListValue::from_iter(["来自", "foo", "bar"]);
        let v2 = ListValue::from_iter(["fizz", "荷兰", "buzz"]);
        let v3 = ListValue::from_iter([None, None, Some("的爱")]);

        assert_eq!(array_access(v1.as_scalar_ref(), 1), Some("来自".into()));
        assert_eq!(array_access(v2.as_scalar_ref(), 2), Some("荷兰".into()));
        assert_eq!(array_access(v3.as_scalar_ref(), 3), Some("的爱".into()));
    }

    #[test]
    fn test_nested_array_access() {
        let v = ListValue::from_scalar_iter(
            &DataType::Varchar.list(),
            [
                ListValue::from_iter(["foo", "bar"]),
                ListValue::from_iter(["fizz", "buzz"]),
            ],
        );
        assert_eq!(
            array_access(v.as_scalar_ref(), 1),
            Some(ListValue::from_iter(["foo", "bar"]).as_scalar_ref().into())
        );
    }
}
