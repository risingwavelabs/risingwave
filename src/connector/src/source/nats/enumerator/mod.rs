// Copyright 2025 RisingWave Labs
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

use async_trait::async_trait;
use risingwave_common::bail;

use super::NatsProperties;
use super::source::{NatsOffset, NatsSplit};
use crate::error::ConnectorResult;
use crate::source::{SourceEnumeratorContextRef, SplitEnumerator, SplitId};

#[derive(Debug, Clone)]
pub struct NatsSplitEnumerator {
    subject: String,
    #[expect(dead_code)]
    split_id: SplitId,
    client: async_nats::Client,
}

#[async_trait]
impl SplitEnumerator for NatsSplitEnumerator {
    type Properties = NatsProperties;
    type Split = NatsSplit;

    async fn new(
        properties: Self::Properties,
        _context: SourceEnumeratorContextRef,
    ) -> ConnectorResult<NatsSplitEnumerator> {
        let client = properties.common.build_client().await?;

        // check if the stream exists or allow create stream
        let jetstream = properties.common.build_context().await?;
        let _ = properties
            .common
            .build_or_get_stream(jetstream, properties.stream.clone())
            .await?;
        Ok(Self {
            subject: properties.common.subject,
            split_id: Arc::from("0"),
            client,
        })
    }

    async fn list_splits(&mut self) -> ConnectorResult<Vec<NatsSplit>> {
        // Nats currently does not support list_splits API, if we simple return the default 0 without checking the client status, will result executor crash
        let state = self.client.connection_state();
        if state != async_nats::connection::State::Connected {
            bail!(
                "Nats connection status is not connected, current status is {:?}",
                state
            );
        }
        // TODO: to simplify the logic, return 1 split for first version
        let nats_split = NatsSplit {
            subject: self.subject.clone(),
            split_id: Arc::from("0"), // be the same as `from_nats_jetstream_message`
            start_sequence: NatsOffset::None,
        };

        Ok(vec![nats_split])
    }
}
