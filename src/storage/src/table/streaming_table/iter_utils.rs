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

use std::borrow::Cow;

use futures::future::{select, Either};
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::Row;

use crate::error::{StorageError, StorageResult};

/// Merge two streams of primary key and rows into a single stream, sorted by primary key.
/// We should ensure that the primary key from different streams are unique.
#[try_stream(ok = (Cow<'a, Row>, Cow<'a, Row>), error = StorageError)]
pub async fn zip_by_order_key<'a, S>(stream1: S, key1: &'a [usize], stream2: S, key2: &'a [usize])
where
    S: Stream<Item = StorageResult<Cow<'a, Row>>> + 'a,
{
    pin_mut!(stream1);
    pin_mut!(stream2);
    'outer: loop {
        let prefer_left: bool = rand::random();
        let select_result = if prefer_left {
            select(stream1.next(), stream2.next()).await
        } else {
            match select(stream2.next(), stream1.next()).await {
                Either::Left(x) => Either::Right(x),
                Either::Right(x) => Either::Left(x),
            }
        };

        match select_result {
            Either::Left((None, _)) | Either::Right((None, _)) => {
                // Return because one side end, no more rows to match
                break;
            }
            Either::Left((Some(row), _)) => {
                let left_row = row?;
                'inner: loop {
                    let right_row = stream2.next().await;
                    match right_row {
                        Some(row) => {
                            let right_row = row?;
                            if Row::eq_by_pk(&left_row, key1, &right_row, key2) {
                                yield (left_row, right_row);
                                break 'inner;
                            }
                        }
                        None => break 'outer,
                    }
                }
            }
            Either::Right((Some(row), _)) => {
                let right_row = row?;
                'inner: loop {
                    let left_row = stream1.next().await;
                    match left_row {
                        Some(row) => {
                            let left_row = row?;
                            if Row::eq_by_pk(&left_row, key1, &right_row, key2) {
                                yield (left_row, right_row);
                                break 'inner;
                            }
                        }
                        None => break 'outer,
                    }
                }
            }
        }
    }
}
