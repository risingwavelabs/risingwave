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



use anyhow::{ Result};
use async_trait::async_trait;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::RwError;
use crate::{ConnectorStateV2, SplitImpl};

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct DatagenSplitReader {}

#[async_trait]
impl SplitReader for DatagenSplitReader {
    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        todo!()
    }
}

impl DatagenSplitReader {
    pub async fn new(properties: DatagenProperties, state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}
