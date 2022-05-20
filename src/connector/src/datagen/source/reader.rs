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

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use super::generator::DatagenEventGenerator;
use crate::datagen::DatagenProperties;
use crate::{Column, ConnectorStateV2, SourceMessage, SplitReader};

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct DatagenSplitReader {
    generator: DatagenEventGenerator,
}

#[async_trait]
impl SplitReader for DatagenSplitReader {
    type Properties = DatagenProperties;

    async fn new(
        properties: DatagenProperties,
        state: ConnectorStateV2,
        columns: Option<Vec<Column>>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let _ = state;
        let batch_chunk_size = properties.max_chunk_size.parse::<u64>()?;
        let rows_per_second = properties.rows_per_second.parse::<u64>()?;

        if let Some(columns) = columns && !columns.is_empty(){
            Ok(DatagenSplitReader {
                generator: DatagenEventGenerator::new(columns, 0, batch_chunk_size, rows_per_second)?,
            })
        } else{
            Err(anyhow!("datagen table's columns is empty or none"))
        }
    }

    async fn next(&mut self) -> Result<Option<Vec<SourceMessage>>> {
        self.generator.next().await
    }
}
