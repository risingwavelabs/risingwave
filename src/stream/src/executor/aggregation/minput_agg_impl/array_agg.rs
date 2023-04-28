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

use risingwave_common::array::ListValue;
use risingwave_common::types::{Datum, DatumRef, ToOwnedDatum};
use smallvec::SmallVec;

use super::MInputAggregator;

pub struct ArrayAgg;

impl MInputAggregator for ArrayAgg {
    type Value = Datum;

    fn convert_cache_value(&self, value: SmallVec<[DatumRef<'_>; 2]>) -> Self::Value {
        value[0].to_owned_datum()
    }

    fn aggregate<'a>(&'a self, values: impl Iterator<Item = &'a Self::Value>) -> Datum {
        let res_values: Vec<Datum> = values.cloned().collect();
        if res_values.is_empty() {
            return None;
        }
        Some(ListValue::new(res_values).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::cache::TopNCache;

    #[test]
    fn test_array_agg_aggregate() {
        let agg = ArrayAgg;

        let mut cache = TopNCache::new(10);
        assert_eq!(agg.aggregate(cache.values()), None);

        cache.insert(vec![1, 2, 3], Some("hello".to_string().into()));
        cache.insert(vec![1, 2, 4], Some("world".to_string().into()));
        assert_eq!(
            agg.aggregate(cache.values()),
            Some(
                ListValue::new(vec![
                    Some("hello".to_string().into()),
                    Some("world".to_string().into()),
                ])
                .into()
            )
        );

        cache.insert(vec![0, 1, 2], Some("emmm".to_string().into()));
        assert_eq!(
            agg.aggregate(cache.values()),
            Some(
                ListValue::new(vec![
                    Some("emmm".to_string().into()),
                    Some("hello".to_string().into()),
                    Some("world".to_string().into()),
                ])
                .into()
            )
        );

        cache.insert(vec![6, 6, 6], None);
        assert_eq!(
            agg.aggregate(cache.values()),
            Some(
                ListValue::new(vec![
                    Some("emmm".to_string().into()),
                    Some("hello".to_string().into()),
                    Some("world".to_string().into()),
                    None,
                ])
                .into()
            )
        );
    }

    #[test]
    fn test_array_agg_convert() {
        let agg = ArrayAgg;
        let args = SmallVec::from_vec(vec![Some("hello".into())]);
        assert_eq!(
            agg.convert_cache_value(args),
            Some("hello".to_string().into())
        );
    }
}
