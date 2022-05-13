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

use anyhow::Result;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Duration};

use crate::SourceMessage;

#[derive(Clone, Debug)]
pub struct DatagenEventGenerator {
    pub last_offset: i32,
    pub batch_chunk_size: i32,
}

impl DatagenEventGenerator {
    pub async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        sleep(Duration::from_secs(5)).await;
        let mut res = vec![];
        for i in 0..self.batch_chunk_size {
            res.push(SourceMessage {
                payload: Some(Bytes::from(serde_json::to_string(&Event::new(i))?)),
                offset: (self.last_offset + i).to_string(),
                split_id: 0.to_string(),
            })
        }
        self.last_offset += self.batch_chunk_size;
        Ok(Some(res))
    }
}

#[derive(Serialize, Deserialize)]
struct Event {
    v1: i32,
    v2: i32,
}
impl Event {
    pub fn new(i: i32) -> Self {
        Self { v1: i, v2: i }
    }
}
