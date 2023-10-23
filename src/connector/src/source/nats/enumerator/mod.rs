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

use std::sync::Arc;

use anyhow;
use async_trait::async_trait;

use super::source::{NatsOffset, NatsSplit};
use super::NatsProperties;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator, SplitId};

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct NatsSplitEnumerator {
    subject: String,
    split_id: SplitId,
}

#[async_trait]
impl SplitEnumerator for NatsSplitEnumerator {
    type Properties = NatsProperties;
    type Split = NatsSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> anyhow::Result<NatsSplitEnumerator> {
        Ok(Self {
            subject: properties.common.subject,
            split_id: Arc::from("0"),
        })
    }

    async fn list_splits(&mut self) -> anyhow::Result<Vec<NatsSplit>> {
        // TODO: to simplify the logic, return 1 split for first version
        let nats_split = NatsSplit {
            subject: self.subject.clone(),
            split_id: Arc::from("0"), // be the same as `from_nats_jetstream_message`
            start_sequence: NatsOffset::None,
        };

        Ok(vec![nats_split])
    }
}
