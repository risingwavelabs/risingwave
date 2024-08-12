// Copyright 2024 RisingWave Labs
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

/// Cache for global approx percentile executor.
/// We use it to maintain the approximated quantile's bucket_id incrementally.
/// Without this, when we flush and get the results on each barrier,
/// we have to re-scan the entire state table to get the approximated quantile.
///
/// Here's what the cache looks like visually:
/// | values in state table | values in cache | values in state table |
/// |        values < x     | x < values < y  |     values > y        |
///
/// You can see that we will maintain a continuous range of values in the cache.
/// We will ALWAYS maintain the approximated percentile value's bucket_id
/// within the range of values in cache, refilling and evicting the cache when necessary.
///
/// ## Terms
/// - `percentile_bucket_id`: The bucket_id corresponding to the approximated percentile.
/// - `percentile_index`: The count offset of the approximated percentile in the sorted values.
///
/// For instance, given the following buckets and their counts,
/// | bucket_id | count |
/// |-----------|-------|
/// |     0     |   10  |
/// |     1     |   20  |
/// |     2     |   30  |
/// |     3     |   40  |
/// we have total row_count = 100.
/// If we want the 10th percentile,
/// the `percentile_index` = row_count * 0.1 = 10.
/// The `percentile_bucket_id` = 1.
///
/// - `cache_size`: The number of buckets to maintain in the cache. Must be at least 1.
/// - `offset_larger_than_percentile` = `cache_size` / 2
/// - `offset_smaller_than_percentile` = `cache_size` - 1 - `offset_larger_than_percentile`
///
/// ## Updating Cache
///
/// ### Empty state
/// Initially, the cache is empty.
/// INSERT: Just fill up the cache.
/// UPDATE/DELETE: Rejected
///
/// ### Bootstrap
/// Iterate over the state table until we get the percentile count.
/// Re-scan `offset_larger_than_percentile`,
/// `offset_smaller_than_percentile` buckets around the `bucket_id` offset.
///
/// ### Adjusting Pointer
/// We consider one value added / removed at a time.
/// We can search around the current bucket id to get the new bucket id.
/// Our pointer will be (bucket_id, offset).
/// and `shift` refers to how much we shift this pointer.
/// For instance, given the following data:
/// | bucket_id | count |
/// |-----------|-------|
/// |     0     |   10  | <--- (bucket_id=0, offset=3)
/// |     1     |   20  |         shift=+31
/// |     2     |   30  | <--- (bucket_id=2, offset=4)
/// |     3     |   40  |
///
/// For `updated prev percentile index`, first we know that prev percentile index,
/// corresponds to some (bucket_id, offset).
/// `updated` simply means after update/delete/insert values, the (bucket_id, offset)
/// should now correspond to some new percentile index.
/// So `updated prev percentile index` refers to this.
///
/// A single `value` refers to `(bucket_id, count=K)` pair.
///
/// NOTE(kwannoel): The smallest precision should be `q`. Anything smaller than that should be rounded off
/// when using math operations with floating offsets, to avoid floating point errors.
///
/// We have to consider a few cases:
/// 1. value bucket_id lhs of pointer bucket_id
///    The pointer shifts towards the left.
///    prev total count = X
///    new total count = X+K
///    prev percentile index = q*X
///    updated prev percentile index = K+q*X
///    new percentile index = q*(X+K)
///    shift = new percentile index - updated prev percentile index
///          = qX + qK - qX - K
///          = K(q-1)
///
/// 2. value bucket_id rhs of pointer bucket_id.
///    The pointer shifts towards the right.
///    prev total count = X
///    new total count = X+K
///    prev percentile index = q*X
///    updated prev percentile index = q*X
///    new percentile index = q*(X+K)
///    shift = new percentile index - updated prev percentile index = qX + qK - qX = qK
///
/// 3. value bucket_id eq to pointer bucket_id.
///    The pointer shifts towards the right.
///    prev total count = X
///    new total count = X+K
///    prev percentile index = q*X
///    updated prev percentile index = q*X
///    new percentile index = q*(X+K)
///    shift = new percentile index - updated prev percentile index = qX + qK - qX = qK
///
/// 6. values removed from bucket_id itself (empty afterwards).
///    The pointer shifts towards the left.
///    prev total count = X
///    new total count = X-1
///    prev percentile index = q*X
///    updated prev percentile index = None
///    new percentile index = q*(X-1)
///    shift = bucket immediately lhs of bucket_id.
///
/// 7. Values added when bucket_id=None, offset=None.
///    bucket_id=value_bucket_id
///    offset = value_count.
struct Cache {
}