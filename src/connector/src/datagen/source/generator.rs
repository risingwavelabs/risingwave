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

use anyhow::Result;
use bytes::Bytes;
use serde_json::{Map, Value};
use tokio::time::{sleep, Duration};

use super::field_generator::FieldGeneratorImpl;
use crate::SourceMessage;

#[derive(Default)]
pub struct DatagenEventGenerator {
    pub fields_map: HashMap<String, FieldGeneratorImpl>,
    pub events_so_far: u64,
    pub batch_chunk_size: u64,
    pub rows_per_second: u64,
    pub split_id: String,
    pub split_num: u64,
    pub split_index: u64,
}

impl DatagenEventGenerator {
    pub fn new(
        fields_map: HashMap<String, FieldGeneratorImpl>,
        rows_per_second: u64,
        events_so_far: u64,
        split_id: String,
        split_num: u64,
        split_index: u64,
    ) -> Result<Self> {
        Ok(Self {
            fields_map,
            events_so_far,
            rows_per_second,
            split_id,
            split_num,
            split_index,
            ..Default::default()
        })
    }

    pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        sleep(Duration::from_secs(1)).await;
        let mut res = vec![];
        let split_index = self.split_index;
        let split_num = self.split_num;
        let addition_one = ((self.rows_per_second % split_num) > split_index) as u64;
        let partition_size = self.rows_per_second / split_num + addition_one;
        for i in 0..partition_size {
            let map: Map<String, Value> = self
                .fields_map
                .iter_mut()
                .map(|(name, field_generator)| (name.to_string(), field_generator.generate()))
                .collect();

            let value = Value::Object(map);
            let msg = SourceMessage {
                payload: Some(Bytes::from(value.to_string())),
                offset: (self.events_so_far + i).to_string(),
                split_id: self.split_id.clone(),
            };

            res.push(msg);
        }
        self.events_so_far += partition_size;
        Ok(Some(res))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datagen::source::field_generator::FieldKind;

    async fn check_partition_result(
        split_num: u64,
        split_index: u64,
        rows_per_second: u64,
        expected_length: usize,
    ) {
        let split_id = format!("{}-{}", split_num, split_index);
        let mut fields_map = HashMap::new();
        fields_map.insert(
            "v1".to_string(),
            FieldGeneratorImpl::new(
                risingwave_common::types::DataType::Int32,
                FieldKind::Sequence,
                Some("1".to_string()),
                Some("10".to_string()),
                split_index,
                split_num,
            )
            .unwrap(),
        );

        fields_map.insert(
            "v2".to_string(),
            FieldGeneratorImpl::new(
                risingwave_common::types::DataType::Float32,
                FieldKind::Sequence,
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

        let chunk = generator.next().await.unwrap().unwrap();
        assert_eq!(expected_length, chunk.len());
        dbg!(chunk);
    }

    #[tokio::test]
    async fn test_one_partition_sequence() {
        check_partition_result(1, 0, 10, 10).await;
    }

    #[tokio::test]
    async fn test_two_partition_sequence() {
        check_partition_result(2, 0, 10, 5).await;
        check_partition_result(2, 1, 10, 5).await;
    }

    #[tokio::test]
    async fn test_three_partition_sequence() {
        check_partition_result(3, 0, 10, 4).await;
        check_partition_result(3, 1, 10, 3).await;
        check_partition_result(3, 2, 10, 3).await;
    }
}
