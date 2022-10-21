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

/// Common aggregator for `min`/`max`. The behavior is simply to choose the
/// first value as aggregation result, so the value order in the given cache
/// is important and should be maintained outside.
pub struct ExtremeAgg;

impl StateCacheAggregator for ExtremeAgg {
    // TODO(yuchao): We can generate an `ExtremeAgg` for each data type to save memory.
    type Value = Datum;

    fn convert_cache_value(&self, value: SmallVec<[DatumRef<'_>; 2]>) -> Self::Value {
        value[0].map(ScalarRefImpl::into_scalar_impl)
    }

    fn aggregate<'a>(&'a self, mut values: impl Iterator<Item = &'a Self::Value>) -> Datum {
        values.next().cloned().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::aggregation::state_cache::cache::OrderedCache;

    #[test]
    fn test_extreme_agg_aggregate() {
        let agg = ExtremeAgg;

        let mut cache = OrderedCache::new(10);
        assert_eq!(agg.aggregate(cache.iter_values()), None);

        cache.insert(vec![1, 2, 3], Some("hello".to_string().into()));
        cache.insert(vec![1, 3, 4], Some("world".to_string().into()));
        assert_eq!(
            agg.aggregate(cache.iter_values()),
            Some("hello".to_string().into())
        );

        cache.insert(vec![0, 1, 2], Some("emmm".to_string().into()));
        assert_eq!(
            agg.aggregate(cache.iter_values()),
            Some("emmm".to_string().into())
        );
    }

    #[test]
    fn test_extreme_agg_convert() {
        let agg = ExtremeAgg;
        let args = SmallVec::from_vec(vec![Some("boom".into())]);
        assert_eq!(
            agg.convert_cache_value(args),
            Some("boom".to_string().into())
        );
        let args = SmallVec::from_vec(vec![Some("hello".into()), Some("world".into())]);
        assert_eq!(
            agg.convert_cache_value(args),
            Some("hello".to_string().into())
        );
    }
}
