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
    fn test_length() {
        let v = ListValue::new(vec![
                Some(ScalarImpl::Utf8("foo".into())),
                Some(ScalarImpl::Utf8("bar".into())),
            ]);
        let l = ListRef::ValueRef { val: &v };

        println!("access: {:?}", array_access::<String>(l, 1));



        // let cases = [
        //     ("hello world", "world", Ok(7)),
        //     ("床前明月光", "月光", Ok(4)),
        //     ("床前明月光", "故乡", Ok(0)),
        // ];

        // for (str, sub_str, expected) in cases {
        //     println!("position is {}", position(str, sub_str).unwrap());
        //     assert_eq!(position(str, sub_str), expected)
        // }
    }
}
