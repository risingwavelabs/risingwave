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

use risingwave_pb::catalog::Sink as ProstSink;

use super::{RelationCatalog, SinkId};
use crate::user::UserId;
use crate::WithOptions;

#[derive(Clone, Debug)]
pub struct SinkCatalog {
    pub id: SinkId,
    pub name: String,

    pub properties: WithOptions,
    pub owner: UserId,
}

impl From<&ProstSink> for SinkCatalog {
    fn from(sink: &ProstSink) -> Self {
        SinkCatalog {
            id: sink.id,
            name: sink.name.clone(),
            properties: WithOptions::new(sink.properties.clone()),
            owner: sink.owner,
        }
    }
}

impl RelationCatalog for SinkCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}
