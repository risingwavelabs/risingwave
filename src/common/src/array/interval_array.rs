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

use super::{PrimitiveArray, PrimitiveArrayBuilder};
use crate::types::interval::IntervalUnit;

pub type IntervalArray = PrimitiveArray<IntervalUnit>;
pub type IntervalArrayBuilder = PrimitiveArrayBuilder<IntervalUnit>;

#[cfg(test)]
mod tests {
    use super::IntervalArray;
    use crate::array::interval_array::{IntervalArrayBuilder, IntervalUnit};
    use crate::array::{Array, ArrayBuilder};

    #[test]
    fn test_interval_array() {
        let cardinality = 5;
        let mut array_builder = IntervalArrayBuilder::new(cardinality);
        for _ in 0..cardinality {
            let v = IntervalUnit::from_ymd(1, 0, 0);
            array_builder.append(Some(v)).unwrap();
        }
        let ret_arr = array_builder.finish();
        for v in ret_arr.iter().flatten() {
            assert_eq!(v.get_years(), 1);
            assert_eq!(v.get_months(), 12);
            assert_eq!(v.get_days(), 0);
        }
        let ret_arr = IntervalArray::from_slice(&[Some(IntervalUnit::from_ymd(1, 0, 0)), None]);
        let v = ret_arr.value_at(0).unwrap();
        assert_eq!(v.get_years(), 1);
        assert_eq!(v.get_months(), 12);
        assert_eq!(v.get_days(), 0);
        let v = ret_arr.value_at(1);
        assert_eq!(v, None);
        let v = unsafe { ret_arr.value_at_unchecked(0).unwrap() };
        assert_eq!(v.get_years(), 1);
        assert_eq!(v.get_months(), 12);
        assert_eq!(v.get_days(), 0);
    }
}
