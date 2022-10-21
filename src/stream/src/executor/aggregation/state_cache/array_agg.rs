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

use risingwave_common::array::ListValue;
use risingwave_common::types::{Datum, DatumRef, ScalarRefImpl};
use smallvec::SmallVec;

use super::StateCacheAggregator;

pub struct ArrayAgg;

impl StateCacheAggregator for ArrayAgg {
    type Value = Datum;

    fn convert_cache_value(&self, value: SmallVec<[DatumRef<'_>; 2]>) -> Self::Value {
        value[0].map(ScalarRefImpl::into_scalar_impl)
    }

    fn aggregate<'a>(&'a self, values: impl Iterator<Item = &'a Self::Value>) -> Datum {
        let mut res_values = Vec::with_capacity(values.size_hint().0);
        for value in values {
            res_values.push(value.clone());
        }
        Some(ListValue::new(res_values).into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::aggregation::state_cache::cache::OrderedCache;

    #[test]
    fn test_array_agg_aggregate() {
        let agg = ArrayAgg;

        let mut cache = OrderedCache::new(10);
        // FIXME(yuchao): the behavior is not compatible with PG, #5962
        assert_eq!(
            agg.aggregate(cache.iter_values()),
            Some(ListValue::new(vec![]).into())
        );

        cache.insert(vec![1, 2, 3], Some("hello".to_string().into()));
        cache.insert(vec![1, 2, 4], Some("world".to_string().into()));
        assert_eq!(
            agg.aggregate(cache.iter_values()),
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
            agg.aggregate(cache.iter_values()),
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
            agg.aggregate(cache.iter_values()),
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
