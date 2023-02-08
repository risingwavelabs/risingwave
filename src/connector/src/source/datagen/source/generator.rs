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
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use bytes::Bytes;
use futures_async_stream::try_stream;
use risingwave_common::field_generator::FieldGeneratorImpl;
use serde_json::Value;

use crate::source::{SourceMessage, SourceMeta, SplitId};

pub struct DatagenEventGenerator {
    fields_map: HashMap<String, FieldGeneratorImpl>,
    offset: u64,
    split_id: SplitId,
    partition_rows_per_second: u64,
}

#[derive(Debug, Clone)]
pub struct DatagenMeta {
    // timestamp(milliseconds) of the data generated
    pub timestamp: Option<i64>,
}

impl DatagenEventGenerator {
    pub fn new(
        fields_map: HashMap<String, FieldGeneratorImpl>,
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
            fields_map,
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
                    let mut fields = serde_json::Map::with_capacity(self.fields_map.len());
                    for (name, field_generator) in &mut self.fields_map {
                        let field = field_generator.generate(self.offset);
                        // sequence generator will return None when it reaches the end
                        if field.is_null() {
                            tracing::info!(
                                "datagen split {} stop generate, offset {}",
                                self.split_id,
                                self.offset
                            );
                            return Ok(());
                        }
                        fields.insert(name.to_string(), field);
                    }
                    let value = Value::Object(fields);
                    msgs.push(SourceMessage {
                        payload: Some(Bytes::from(value.to_string())),
                        offset: self.offset.to_string(),
                        split_id: self.split_id.clone(),
                        meta: SourceMeta::Datagen(DatagenMeta {
                            timestamp: Some(
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as i64,
                            ),
                        }),
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
        let start = 1;
        let end = 10;
        let mut fields_map = HashMap::new();
        fields_map.insert(
            "v1".to_string(),
            FieldGeneratorImpl::with_number_sequence(
                risingwave_common::types::DataType::Int32,
                Some(start.to_string()),
                Some(end.to_string()),
                split_index,
                split_num,
            )
            .unwrap(),
        );

        fields_map.insert(
            "v2".to_string(),
            FieldGeneratorImpl::with_number_sequence(
                risingwave_common::types::DataType::Float32,
                Some(start.to_string()),
                Some(end.to_string()),
                split_index,
                split_num,
            )
            .unwrap(),
        );

        let generator = DatagenEventGenerator::new(
            fields_map,
            rows_per_second,
            0,
            split_id,
            split_num,
            split_index,
        )
        .unwrap();

        let mut stream = generator.into_stream().boxed();

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(expected_length, chunk.len());

        let empty_chunk = stream.next().await;
        if rows_per_second >= (end - start + 1) {
            assert!(empty_chunk.is_none());
        } else {
            assert!(empty_chunk.is_some());
        }
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
