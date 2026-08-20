// Copyright 2026 RisingWave Labs
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

use risingwave_common::types::JsonbVal;
use serde::{Deserialize, Serialize};

use crate::error::ConnectorResult;
use crate::source::{SplitId, SplitMetaData};

/// The fixed id of the single split of a `WebSocket` source.
pub const WEBSOCKET_SPLIT_ID: &str = "websocket";

/// A `WebSocket` source always has exactly one split, since a `WebSocket` connection is a
/// single ordered stream of messages without any partitioning or replay capability.
///
/// The split is persisted to the checkpoint, but there is no offset to restore: on
/// recovery the reader simply reconnects and consumes messages from that point on.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Hash)]
pub struct WebsocketSplit {}

impl SplitMetaData for WebsocketSplit {
    fn id(&self) -> SplitId {
        WEBSOCKET_SPLIT_ID.into()
    }

    fn restore_from_json(value: JsonbVal) -> ConnectorResult<Self> {
        serde_json::from_value(value.take()).map_err(Into::into)
    }

    fn encode_to_json(&self) -> JsonbVal {
        serde_json::to_value(self.clone()).unwrap().into()
    }

    fn update_offset(&mut self, _last_seen_offset: String) -> ConnectorResult<()> {
        // A WebSocket stream cannot be replayed from an offset, so there is nothing to
        // persist here. The offset attached to messages is a per-reader counter only
        // used for observability.
        Ok(())
    }
}

impl WebsocketSplit {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for WebsocketSplit {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_json_round_trip() {
        let split = WebsocketSplit::new();
        let json = split.encode_to_json();
        let restored = WebsocketSplit::restore_from_json(json).unwrap();
        assert_eq!(split, restored);
        assert_eq!(restored.id().as_ref(), WEBSOCKET_SPLIT_ID);
    }

    #[test]
    fn test_update_offset_is_noop() {
        let mut split = WebsocketSplit::new();
        split.update_offset("42".to_owned()).unwrap();
        assert_eq!(split, WebsocketSplit::new());
    }
}
