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

use std::borrow::BorrowMut;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use futures::StreamExt;
use pulsar::{Consumer, Pulsar, TokioExecutor};

use crate::base::{SourceMessage, SourceReader};
use crate::pulsar::split::{PulsarOffset, PulsarSplit};
use crate::{ConnectorStateV2, Properties};

pub struct PulsarSplitReader {
    pulsar: Pulsar<TokioExecutor>,
    consumer: Consumer<Vec<u8>, TokioExecutor>,
    split: PulsarSplit,
}

const PULSAR_MAX_FETCH_MESSAGES: u32 = 1024;

#[async_trait]
impl SourceReader for PulsarSplitReader {
    async fn next(&mut self) -> anyhow::Result<Option<Vec<SourceMessage>>> {
        let mut stream = self
            .consumer
            .borrow_mut()
            .ready_chunks(PULSAR_MAX_FETCH_MESSAGES as usize);

        let chunk = match stream.next().await {
            None => return Ok(None),
            Some(chunk) => chunk,
        };

        let mut ret = Vec::with_capacity(chunk.len());

        for msg in chunk {
            let msg = msg.map_err(|e| anyhow!(e))?;

            let entry_id = msg.message_id.id.entry_id;

            let should_stop = match self.split.stop_offset {
                PulsarOffset::MessageID(id) => entry_id >= id,
                PulsarOffset::Timestamp(timestamp) => {
                    msg.payload.metadata.event_time() >= timestamp
                }
                PulsarOffset::None => false,
            };

            if should_stop {
                self.consumer
                    .borrow_mut()
                    .unsubscribe()
                    .await
                    .map_err(|e| anyhow!(e))?;
                break;
            }

            ret.push(SourceMessage::from(msg));
        }

        Ok(Some(ret))
    }

    async fn new(_props: Properties, _state: ConnectorStateV2) -> Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }
}
