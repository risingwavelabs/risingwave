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
use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use futures_async_stream::try_stream;
use risingwave_common::field_generator::FieldGeneratorImpl;
use serde_json::{Map, Value};

use super::DEFAULT_DATAGEN_INTERVAL;
use crate::source::{SourceMessage, SplitId};

pub struct DatagenEventGenerator {
    pub fields_map: HashMap<String, FieldGeneratorImpl>,
    pub events_so_far: u64,
    pub rows_per_second: u64,
    pub split_id: SplitId,
    pub partition_size: u64,
}

impl DatagenEventGenerator {
    pub fn new(
        fields_map: HashMap<String, FieldGeneratorImpl>,
        rows_per_second: u64,
        events_so_far: u64,
        split_id: SplitId,
        split_num: u64,
        split_index: u64,
    ) -> Result<Self> {
        let partition_size = if rows_per_second % split_num > split_index {
            rows_per_second / split_num + 1
        } else {
            rows_per_second / split_num
        };
        Ok(Self {
            fields_map,
            events_so_far,
            rows_per_second,
            split_id,
            partition_size,
        })
    }

    #[try_stream(ok = Vec<SourceMessage>, error = anyhow::Error)]
    pub async fn into_stream(mut self) {
        loop {
            yield self.next().await?
        }
    }

    pub async fn next(&mut self) -> Result<Vec<SourceMessage>> {
        let now = Instant::now();
        let mut res = vec![];
        let mut generated_count: u64 = 0;
        // if generating data time beyond 1s then just return the result
        for i in 0..self.partition_size {
            if now.elapsed().as_millis() >= DEFAULT_DATAGEN_INTERVAL {
                break;
            }
            let offset = self.events_so_far + i;
            let map: Map<String, Value> = self
                .fields_map
                .iter_mut()
                .map(|(name, field_generator)| (name.to_string(), field_generator.generate(offset)))
                .collect();

            let value = Value::Object(map);
            let msg = SourceMessage {
                payload: Some(Bytes::from(value.to_string())),
                offset: offset.to_string(),
                split_id: self.split_id.clone(),
            };
            generated_count += 1;
            res.push(msg);
        }

        self.events_so_far += generated_count;

        // if left time < 1s then wait
        if now.elapsed().as_millis() < DEFAULT_DATAGEN_INTERVAL {
            tokio::time::sleep(Duration::from_millis(
                (DEFAULT_DATAGEN_INTERVAL - now.elapsed().as_millis()) as u64,
            ))
            .await;
        }

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn check_sequence_partition_result(
        split_num: u64,
        split_index: u64,
        rows_per_second: u64,
        expected_length: usize,
    ) {
        let split_id = format!("{}-{}", split_num, split_index).into();
        let mut fields_map = HashMap::new();
        fields_map.insert(
            "v1".to_string(),
            FieldGeneratorImpl::with_sequence(
                risingwave_common::types::DataType::Int32,
                Some("1".to_string()),
                Some("10".to_string()),
                split_index,
                split_num,
            )
            .unwrap(),
        );

        fields_map.insert(
            "v2".to_string(),
            FieldGeneratorImpl::with_sequence(
                risingwave_common::types::DataType::Float32,
                Some("1".to_string()),
                Some("10".to_string()),
                split_index,
                split_num,
            )
            .unwrap(),
        );

        let mut generator = DatagenEventGenerator::new(
            fields_map,
            rows_per_second,
            0,
            split_id,
            split_num,
            split_index,
        )
        .unwrap();

        let chunk = generator.next().await.unwrap();
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
