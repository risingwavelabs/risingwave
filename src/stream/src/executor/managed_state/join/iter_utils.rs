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
use risingwave_storage::table::error::{StateTableError, StateTableResult};

/// Zip two streams of primary key and rows into a single stream, sorted by order key.
/// We should ensure that the order key from different streams are unique.
#[try_stream(ok = (Cow<'a, Row>, Cow<'a, Row>), error = StateTableError)]
pub async fn zip_by_order_key<'a, S>(stream1: S, stream2: S)
where
    S: Stream<Item = StateTableResult<(Cow<'a, Vec<u8>>, Cow<'a, Row>)>> + 'a,
{
    let (stream1, stream2) = (stream1.peekable(), stream2.peekable());
    pin_mut!(stream1);
    pin_mut!(stream2);

    loop {
        match join(stream1.as_mut().peek(), stream2.as_mut().peek()).await {
            (None, _) | (_, None) => break,

            (Some(Ok((left_key, _))), Some(Ok((right_key, _)))) => match left_key.cmp(right_key) {
                Ordering::Greater => {
                    stream2.next().await;
                }
                Ordering::Less => {
                    stream1.next().await;
                }
                Ordering::Equal => {
                    let row_l = stream1
                        .next()
                        .await
                        .unwrap()
                        .map_err(StateTableError::state_table_row_iterator_error)?
                        .1;
                    let row_r = stream2
                        .next()
                        .await
                        .unwrap()
                        .map_err(StateTableError::state_table_row_iterator_error)?
                        .1;
                    yield (row_l, row_r);
                }
            },

            (Some(Err(_)), Some(_)) => {
                // Throw the left error.
                return Err(stream1
                    .next()
                    .await
                    .unwrap()
                    .map_err(StateTableError::state_table_row_iterator_error)
                    .unwrap_err());
            }
            (Some(_), Some(Err(_))) => {
                // Throw the right error.
                return Err(stream2
                    .next()
                    .await
                    .unwrap()
                    .map_err(StateTableError::state_table_row_iterator_error)
                    .unwrap_err());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_async_stream::for_await;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    fn gen_row_with_pk(i: i64) -> StateTableResult<(Cow<'static, Vec<u8>>, Cow<'static, Row>)> {
        Ok((
            Cow::Owned(i.to_be_bytes().to_vec()),
            Cow::Owned(Row(vec![Some(ScalarImpl::Int64(i))])),
        ))
    }

    #[tokio::test]
    async fn test_zip_by_order_key() {
        let stream1 = futures::stream::iter(vec![
            gen_row_with_pk(0),
            gen_row_with_pk(3),
            gen_row_with_pk(6),
            gen_row_with_pk(9),
        ]);

        let stream2 = futures::stream::iter(vec![
            gen_row_with_pk(2),
            gen_row_with_pk(3),
            gen_row_with_pk(9),
            gen_row_with_pk(10),
        ]);

        let zipped = zip_by_order_key(stream1, stream2);

        let expected_results = vec![3, 9];

        #[for_await]
        for (i, result) in zipped.enumerate() {
            let (res0, res1) = result.unwrap();
            let expected_res = gen_row_with_pk(expected_results[i]).unwrap();
            assert_eq!(res0, expected_res.1);
            assert_eq!(res1, expected_res.1);
        }
    }
}
