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

use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_async_stream::try_stream;
use risingwave_common::field_generator::FieldGeneratorImpl;
use risingwave_common::row::OwnedRow;

use crate::source::{SourceMessage, SplitId};

pub struct DatagenEventGenerator {
    fields_vec: Vec<FieldGeneratorImpl>,
    offset: u64,
    split_id: SplitId,
    partition_rows_per_second: u64,
}

impl DatagenEventGenerator {
    pub fn new(
        fields_vec: Vec<FieldGeneratorImpl>,
        rows_per_second: u64,
        offset: u64,
        split_id: SplitId,
        split_num: u64,
        split_index: u64,
    ) -> Result<Self> {
        let partition_rows_per_second = if rows_per_second % split_num > split_index {
            rows_per_second / split_num + 1
        } else {
            rows_per_second / split_num
        };
        Ok(Self {
            fields_vec,
            offset,
            split_id,
            partition_rows_per_second,
        })
    }

    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn into_stream(mut self) {
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        const MAX_ROWS_PER_YIELD: u64 = 1024;
        loop {
            // generate `partition_rows_per_second` rows per second
            interval.tick().await;
            let mut rows_generated_this_second = 0;
            while rows_generated_this_second < self.partition_rows_per_second {
                let mut msgs = vec![];
                let num_rows_to_generate = std::cmp::min(
                    MAX_ROWS_PER_YIELD,
                    self.partition_rows_per_second - rows_generated_this_second,
                );
                for _ in 0..num_rows_to_generate {
                    let data = self
                        .fields_vec
                        .iter_mut()
                        .map(|field_generator| field_generator.generate_datum(self.offset))
                        .collect();
                    // Leak the memory.
                    // We will reclaim the ownership of this piece of memory during parsing.
                    // see `NativeParser`
                    let value = unsafe {
                        let row = OwnedRow::new(data);
                        let row = Box::new(row);
                        let ptr = Box::into_raw(row);
                        let ptr = std::slice::from_raw_parts(
                            (ptr as *const OwnedRow) as *const u8,
                            ::std::mem::size_of::<OwnedRow>(),
                        );
                        ptr
                    };
                    msgs.push(SourceMessage {
                        payload: Some(Bytes::from_static(value)),
                        offset: self.offset.to_string(),
                        split_id: self.split_id.clone(),
                    });
                    self.offset += 1;
                    rows_generated_this_second += 1;
                }
                yield msgs;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::stream::StreamExt;

    use super::*;

    async fn check_sequence_partition_result(
        split_num: u64,
        split_index: u64,
        rows_per_second: u64,
        expected_length: usize,
    ) {
        let split_id = format!("{}-{}", split_num, split_index).into();
        let generator1 = FieldGeneratorImpl::with_number_sequence(
            risingwave_common::types::DataType::Int32,
            Some("1".to_string()),
            Some("10".to_string()),
            split_index,
            split_num,
        )
        .unwrap();
        let generator2 = FieldGeneratorImpl::with_number_sequence(
            risingwave_common::types::DataType::Float32,
            Some("1".to_string()),
            Some("10".to_string()),
            split_index,
            split_num,
        )
        .unwrap();

        let generator = DatagenEventGenerator::new(
            vec![generator1, generator2],
            rows_per_second,
            0,
            split_id,
            split_num,
            split_index,
        )
        .unwrap();

        let chunk = generator
            .into_stream()
            .boxed()
            .next()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(expected_length, chunk.len());
    }

    #[tokio::test]
    async fn test_one_partition_sequence() {
        check_sequence_partition_result(1, 0, 10, 10).await;
    }

    #[tokio::test]
    async fn test_two_partition_sequence() {
        check_sequence_partition_result(2, 0, 10, 5).await;
        check_sequence_partition_result(2, 1, 10, 5).await;
    }

    #[tokio::test]
    async fn test_three_partition_sequence() {
        check_sequence_partition_result(3, 0, 10, 4).await;
        check_sequence_partition_result(3, 1, 10, 3).await;
        check_sequence_partition_result(3, 2, 10, 3).await;
    }
}
