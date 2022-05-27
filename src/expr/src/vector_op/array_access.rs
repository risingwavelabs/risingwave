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

use risingwave_common::error::Result;
use risingwave_common::array::ListRef;
use risingwave_common::types::{Scalar, ScalarImpl, ScalarRef, Datum, DatumRef, ToOwnedDatum};


// TODO(nanderstabel): Clean
#[inline(always)]
pub fn array_access<'a, T: Scalar>(l: ListRef, index: i32) -> Result<T> {
    let temp = l.value_at(index as usize)?;
    let temp = temp.to_owned_datum().unwrap();
    temp.try_into()
}

#[cfg(test)]
mod tests {

    use super::*;
    use risingwave_common::types::ScalarImpl;
    use risingwave_common::array::ListValue;

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
        let v3 = ListValue::new(vec![
            None,
            None,
            Some(ScalarImpl::Utf8("的爱".into())),
        ]);

        let l1 = ListRef::ValueRef { val: &v1 };
        let l2 = ListRef::ValueRef { val: &v2 };
        let l3 = ListRef::ValueRef { val: &v3 };

        assert_eq!(array_access::<String>(l1, 0), Ok("来自".into()));
        assert_eq!(array_access::<String>(l2, 1), Ok("荷兰".into()));
        assert_eq!(array_access::<String>(l3, 2), Ok("的爱".into()));
    }
}
