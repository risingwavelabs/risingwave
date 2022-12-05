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

use async_trait::async_trait;

use crate::source::cdc::{CdcProperties, CdcSplit};
use crate::source::SplitEnumerator;

pub const MYSQL_CDC_PREFIX: &str = "RW_CDC_";

#[derive(Debug)]
pub struct DebeziumSplitEnumerator {
    /// The source_id in the catalog
    source_id: u32,
    /// Debezium will assign a partition identifier for each table
    partition: String,
}

#[async_trait]
impl SplitEnumerator for DebeziumSplitEnumerator {
    type Properties = CdcProperties;
    type Split = CdcSplit;

    async fn new(props: CdcProperties) -> anyhow::Result<DebeziumSplitEnumerator> {
        let partition = format!(
            "{}{}.{}",
            MYSQL_CDC_PREFIX, props.database_name, props.table_name
        );
        Ok(Self {
            source_id: props.source_id,
            partition,
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<CdcSplit>> {
        // CDC source only supports single split
        let splits = vec![CdcSplit {
            source_id: self.source_id,
            partition: self.partition.clone(),
            start_offset: None,
        }];
        Ok(splits)
    }
}
