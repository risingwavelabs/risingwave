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

use risingwave_common::types::{Datum, DatumRef, ScalarRefImpl};
use smallvec::SmallVec;

use super::StateCacheAggregator;

pub struct StringAggData {
    delim: String,
    value: String,
}

pub struct StringAgg;

impl StateCacheAggregator for StringAgg {
    type Value = StringAggData;

    fn convert_cache_value(&self, value: SmallVec<[DatumRef<'_>; 2]>) -> Self::Value {
        StringAggData {
            delim: value[1]
                .map(ScalarRefImpl::into_utf8)
                .unwrap_or_default()
                .to_string(),
            value: value[0]
                .map(ScalarRefImpl::into_utf8)
                .unwrap_or_default()
                .to_string(),
        }
    }

    fn aggregate<'a>(&'a self, mut values: impl Iterator<Item = &'a Self::Value>) -> Datum {
        let mut result = match values.next() {
            Some(data) => data.value.clone(),
            None => return None, // return NULL if no rows to aggregate
        };
        for StringAggData { value, delim } in values {
            result.push_str(delim);
            result.push_str(value);
        }
        Some(result.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::aggregation::state_cache::cache::OrderedCache;

    #[test]
    fn test_string_agg_aggregate() {
        let agg = StringAgg;

        let mut cache = OrderedCache::new(10);
        assert_eq!(agg.aggregate(cache.iter_values()), None);

        cache.insert(
            vec![1, 2, 3],
            StringAggData {
                delim: "_".to_string(),
                value: "hello".to_string(),
            },
        );
        cache.insert(
            vec![1, 3, 4],
            StringAggData {
                delim: ",".to_string(),
                value: "world".to_string(),
            },
        );
        assert_eq!(
            agg.aggregate(cache.iter_values()),
            Some("hello,world".to_string().into())
        );

        cache.insert(
            vec![0, 1, 2],
            StringAggData {
                delim: "/".to_string(),
                value: "emmm".to_string(),
            },
        );
        assert_eq!(
            agg.aggregate(cache.iter_values()),
            Some("emmm_hello,world".to_string().into())
        );
    }

    #[test]
    fn test_string_agg_convert() {
        let agg = StringAgg;
        let args = SmallVec::from_vec(vec![Some("hello".into()), Some("world".into())]);
        let value = agg.convert_cache_value(args);
        assert_eq!(value.value, "hello".to_string());
        assert_eq!(value.delim, "world".to_string());
    }
}
