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

use risingwave_common::array::ListRef;
use risingwave_expr_macro::function;

use crate::Result;

#[function("array_upper(list, int64) -> int64")]
pub fn array_upper(list: ListRef<'_>, dimension: i64) -> Result<Option<i64>> {
    if dimension < 1 {
        return Ok(None);
    }
    let upper_bound = list.upper_bound(dimension as usize);
    Ok(upper_bound.map(|x| x as i64))
}

#[cfg(test)]
mod tests {

    use risingwave_common::array::ListValue;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    #[test]
    fn test_int32_array_upper() {
        let v1 = ListValue::new(vec![]);
        let v2 = ListValue::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Int32(8)),
            Some(ScalarImpl::Int32(3)),
            Some(ScalarImpl::Int32(7)),
        ]);
        let v3 = ListValue::new(vec![
            Some(ScalarImpl::Int32(1)),
            Some(ScalarImpl::Int32(2)),
            Some(ScalarImpl::Int32(3)),
            Some(ScalarImpl::Int32(8)),
            Some(ScalarImpl::Int32(11)),
        ]);

        let l1 = ListRef::ValueRef { val: &v1 };
        assert_eq!(array_upper(l1, 1).unwrap(), None);
        let l2 = ListRef::ValueRef { val: &v2 };
        assert_eq!(array_upper(l2, 1).unwrap(), Some(4));
        let l3 = ListRef::ValueRef { val: &v3 };
        assert_eq!(array_upper(l3, 2).unwrap(), None);

        let v4 = ListValue::new(vec![Some(ScalarImpl::List(v2)), Some(ScalarImpl::List(v3))]);
        let l4 = ListRef::ValueRef { val: &v4 };
        assert_eq!(array_upper(l4, 2).unwrap(), Some(5));
    }
}
