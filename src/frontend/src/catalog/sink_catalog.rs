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

use risingwave_pb::catalog::Sink as ProstSink;

use super::SinkId;
use crate::catalog::TableId;

#[derive(Clone, Debug)]
pub struct SinkCatalog {
    pub id: SinkId,
    pub name: String,

    pub associated_table_id: TableId,
    pub properties: HashMap<String, String>,
    pub owner: u32,
}

impl From<&ProstSink> for SinkCatalog {
    fn from(sink: &ProstSink) -> Self {
        SinkCatalog {
            id: sink.id,
            name: sink.name.clone(),
            associated_table_id: TableId::new(sink.associated_table_id),
            properties: sink.properties.clone(),
            owner: sink.owner,
        }
    }
}
