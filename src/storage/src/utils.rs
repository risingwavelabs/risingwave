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

use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::{Bound, RangeBounds};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::HummockEpoch;

pub type BytesFullKey = FullKey<Bytes>;
pub type BytesFullKeyRange = (Bound<BytesFullKey>, Bound<BytesFullKey>);

pub(crate) fn to_full_key_range<R, B>(table_id: TableId, table_key_range: R) -> BytesFullKeyRange
where
    R: RangeBounds<B> + Send,
    B: AsRef<[u8]>,
{
    let start = match table_key_range.start_bound() {
        Included(k) => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            HummockEpoch::MAX,
        )),
        Excluded(k) => Excluded(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            0,
        )),
        Unbounded => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(b"".to_vec())),
            HummockEpoch::MAX,
        )),
    };
    let end = match table_key_range.end_bound() {
        Included(k) => Included(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            0,
        )),
        Excluded(k) => Excluded(FullKey::new(
            table_id,
            TableKey(Bytes::from(k.as_ref().to_vec())),
            HummockEpoch::MAX,
        )),
        Unbounded => {
            if let Some(next_table_id) = table_id.table_id().checked_add(1) {
                Excluded(FullKey::new(
                    next_table_id.into(),
                    TableKey(Bytes::from(b"".to_vec())),
                    HummockEpoch::MAX,
                ))
            } else {
                Unbounded
            }
        }
    };
    (start, end)
}
