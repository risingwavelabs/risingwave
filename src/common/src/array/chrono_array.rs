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
use crate::types::{NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper};

pub type NaiveDateArray = PrimitiveArray<NaiveDateWrapper>;
pub type NaiveTimeArray = PrimitiveArray<NaiveTimeWrapper>;
pub type NaiveDateTimeArray = PrimitiveArray<NaiveDateTimeWrapper>;

pub type NaiveDateArrayBuilder = PrimitiveArrayBuilder<NaiveDateWrapper>;
pub type NaiveTimeArrayBuilder = PrimitiveArrayBuilder<NaiveTimeWrapper>;
pub type NaiveDateTimeArrayBuilder = PrimitiveArrayBuilder<NaiveDateTimeWrapper>;

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::array::{Array, ArrayBuilder};

    #[test]
    fn test_naivedate_builder() {
        let v = (0..1000)
            .map(NaiveDateWrapper::with_days)
            .map(|x| x.ok())
            .collect_vec();
        let mut builder = NaiveDateArrayBuilder::new(0);
        for i in &v {
            builder.append(*i).unwrap();
        }
        let a = builder.finish();
        let res = v.iter().zip_eq(a.iter()).all(|(a, b)| *a == b);
        assert!(res)
    }

    #[test]
    fn test_naivedate_array_to_protobuf() {
        let input = vec![
            NaiveDateWrapper::with_days(12345).ok(),
            None,
            NaiveDateWrapper::with_days(67890).ok(),
        ];

        let array = NaiveDateArray::from_slice(&input);
        let buffers = array.to_protobuf().values;

        assert_eq!(buffers.len(), 1);

        let output_buffer = input.iter().fold(Vec::new(), |mut v, d| match d {
            Some(d) => {
                d.to_protobuf(&mut v).unwrap();
                v
            }
            None => v,
        });

        assert_eq!(buffers[0].get_body(), &output_buffer);
    }
}
