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

use super::field_generator::{FieldGeneratorImpl, NumericFieldGenerator,FieldKind};
use crate::SourceMessage;

#[derive(Default)]
pub struct DatagenEventGenerator {
    pub fields_map: HashMap<String, FieldGeneratorImpl>,
    pub events_so_far: u64,
    pub batch_chunk_size: u64,
    pub rows_per_second: u64,
    pub split_id: String,
    pub split_num: i32,
    pub split_index: i32,

}

impl DatagenEventGenerator {
    pub fn new(
        fields_map: HashMap<String, FieldGeneratorImpl>,
        rows_per_second: u64,
        events_so_far:u64,
        split_id: String,
        split_num: i32,
        split_index: i32,
    ) -> Result<Self> {
        Ok(Self {
            fields_map,
            rows_per_second,
            events_so_far,
            split_id,
            split_num,
            split_index,
            ..Default::default()
        })
    }

    pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        sleep(Duration::from_secs(
           1
        ))
        .await;
        let mut res = vec![];
        let split_index:u64 = self.split_index as u64 ;
        let split_num:u64 = self.split_num as u64;
        let addition_one:u64 = if self.rows_per_second%split_num > split_index {1} else{0};
        let partition_size = self.rows_per_second/split_num + addition_one;
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
        dbg!(res.len());
        dbg!(split_index);
        Ok(Some(res))

        
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn test_partition_sequence() {
        let mut fields_map = HashMap::new();
        fields_map.insert("v1".to_string(),FieldGeneratorImpl::new(
            risingwave_common::types::DataType::Int32,
            FieldKind::Sequence,
            Some("1".to_string()),
            Some("10".to_string()),
            1,
            2
        ).unwrap());

        let mut generator = DatagenEventGenerator::new(fields_map,
        2,0,"2-1".to_string(),2,1).unwrap();
        

        let first_chunk = generator.next().await.unwrap().unwrap();
        let second_chunk = generator.next().await.unwrap().unwrap();
    }
}