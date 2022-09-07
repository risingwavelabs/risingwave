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
use std::cmp::Ordering;

use futures::future::join;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::Row;
use risingwave_storage::error::{StorageError, StorageResult};

/// Zip two streams of primary key and rows into a single stream, sorted by primary key.
/// We should ensure that the primary key from different streams are unique.
#[try_stream(ok = (Cow<'a, Row>, Cow<'a, Row>), error = StorageError)]
pub async fn zip_by_order_key<'a, S>(stream1: S, key1: &'a [usize], stream2: S, key2: &'a [usize])
where
    S: Stream<Item = StorageResult<Cow<'a, Row>>> + 'a,
{
    let (stream1, stream2) = (stream1.peekable(), stream2.peekable());
    pin_mut!(stream1);
    pin_mut!(stream2);

    loop {
        match join(stream1.as_mut().peek(), stream2.as_mut().peek()).await {
            (None, _) | (_, None) => break,

            (Some(Ok(left_row)), Some(Ok(right_row))) => {
                match Row::cmp_by_key(left_row, key1, right_row, key2) {
                    Ordering::Greater => {
                        stream2.next().await;
                    }
                    Ordering::Less => {
                        stream1.next().await;
                    }
                    Ordering::Equal => {
                        let row_l = stream1.next().await.unwrap()?;
                        let row_r = stream2.next().await.unwrap()?;
                        yield (row_l, row_r);
                    }
                }
            }

            (Some(Err(_)), Some(_)) => {
                // Throw the left error.
                return Err(stream1.next().await.unwrap().unwrap_err());
            }
            (Some(_), Some(Err(_))) => {
                // Throw the right error.
                return Err(stream2.next().await.unwrap().unwrap_err());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use risingwave_common::types::ScalarImpl;
    use risingwave_storage::error::StorageResult;

    use super::*;

    fn gen_row(i: i64) -> StorageResult<Cow<'static, Row>> {
        Ok(Cow::Owned(Row(vec![Some(ScalarImpl::Int64(i))])))
    }

    #[tokio::test]
    async fn test_zip_by_order_key() {
        let stream1 = futures::stream::iter(vec![gen_row(0), gen_row(3), gen_row(6), gen_row(9)]);

        let stream2 = futures::stream::iter(vec![gen_row(2), gen_row(3), gen_row(9), gen_row(10)]);

        let zipped = zip_by_order_key(stream1, &[0], stream2, &[0]);

        let expected_results = vec![3, 9];

        #[for_await]
        for (i, result) in zipped.enumerate() {
            let (res0, res1) = result.unwrap();
            let expected_res = gen_row(expected_results[i]).unwrap();
            assert_eq!(res0, expected_res);
            assert_eq!(res1, expected_res);
        }
    }
}
