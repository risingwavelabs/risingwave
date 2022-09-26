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

use risingwave_common::array::ListRef;
use risingwave_common::types::{Scalar, ToOwnedDatum};

use crate::Result;

#[inline(always)]
pub fn array_access<T: Scalar>(l: Option<ListRef<'_>>, r: Option<i32>) -> Result<Option<T>> {
    match (l, r) {
        // index must be greater than 0 following a one-based numbering convention for arrays
        (Some(list), Some(index)) if index > 0 => {
            let datumref = list.value_at(index as usize)?;
            if let Some(scalar) = datumref.to_owned_datum() {
                Ok(Some(scalar.try_into()?))
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::array::ListValue;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    #[test]
    fn test_int32_array_access() {
        let v1 = ListValue::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int32(3)),
        ]);
        let l1 = ListRef::ValueRef { val: &v1 };

        assert_eq!(array_access::<i32>(Some(l1), Some(1)).unwrap(), Some(1));
        assert_eq!(array_access::<i32>(Some(l1), Some(-1)).unwrap(), None);
        assert_eq!(array_access::<i32>(Some(l1), Some(0)).unwrap(), None);
        assert_eq!(array_access::<i32>(Some(l1), Some(4)).unwrap(), None);
    }

    #[test]
    fn test_utf8_array_access() {
        let v1 = ListValue::new(vec![
            Some(ScalarImpl::Utf8("来自".into())),
            Some(ScalarImpl::Utf8("foo".into())),
            Some(ScalarImpl::Utf8("bar".into())),
        ]);
        let v2 = ListValue::new(vec![
            Some(ScalarImpl::Utf8("fizz".into())),
            Some(ScalarImpl::Utf8("荷兰".into())),
            Some(ScalarImpl::Utf8("buzz".into())),
        ]);
        let v3 = ListValue::new(vec![None, None, Some(ScalarImpl::Utf8("的爱".into()))]);

        let l1 = ListRef::ValueRef { val: &v1 };
        let l2 = ListRef::ValueRef { val: &v2 };
        let l3 = ListRef::ValueRef { val: &v3 };

        assert_eq!(
            array_access::<String>(Some(l1), Some(1)).unwrap(),
            Some("来自".into())
        );
        assert_eq!(
            array_access::<String>(Some(l2), Some(2)).unwrap(),
            Some("荷兰".into())
        );
        assert_eq!(
            array_access::<String>(Some(l3), Some(3)).unwrap(),
            Some("的爱".into())
        );
    }

    #[test]
    fn test_nested_array_access() {
        let v = ListValue::new(vec![
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::Utf8("foo".into())),
                Some(ScalarImpl::Utf8("bar".into())),
            ]))),
            Some(ScalarImpl::List(ListValue::new(vec![
                Some(ScalarImpl::Utf8("fizz".into())),
                Some(ScalarImpl::Utf8("buzz".into())),
            ]))),
        ]);
        let l = ListRef::ValueRef { val: &v };
        assert_eq!(
            array_access::<ListValue>(Some(l), Some(1)).unwrap(),
            Some(ListValue::new(vec![
                Some(ScalarImpl::Utf8("foo".into())),
                Some(ScalarImpl::Utf8("bar".into())),
            ]))
        );
    }
}
