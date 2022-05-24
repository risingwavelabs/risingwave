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

use super::field_generator::{FieldGeneratorImpl, NumericFieldGenerator};
use crate::SourceMessage;
pub type BoxedFieldGenerator = Box<dyn NumericFieldGenerator>;

pub struct DatagenEventGenerator {
    pub fields_map: HashMap<String, FieldGeneratorImpl>,
    pub last_offset: u64,
    pub batch_chunk_size: u64,
    pub rows_per_second: u64,
}

impl DatagenEventGenerator {
    pub fn new(
        fields_map: HashMap<String, FieldGeneratorImpl>,
        last_offset: u64,
        batch_chunk_size: u64,
        rows_per_second: u64,
    ) -> Result<Self> {
        Ok(Self {
            fields_map,
            last_offset,
            batch_chunk_size,
            rows_per_second,
        })
    }

    pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        sleep(Duration::from_secs(
            self.batch_chunk_size / self.rows_per_second,
        ))
        .await;
        let mut res = vec![];
        for i in 0..self.batch_chunk_size {
            let map: Map<String, Value> = self
                .fields_map
                .iter_mut()
                .map(|(name, field_generator)| (name.to_string(), field_generator.generate()))
                .collect();

            let value = Value::Object(map);
            let msg = SourceMessage {
                payload: Some(Bytes::from(value.to_string())),
                offset: (self.last_offset + i).to_string(),
                split_id: 0.to_string(),
            };

            res.push(msg);
        }
        self.last_offset += self.batch_chunk_size;
        Ok(Some(res))
    }
}
