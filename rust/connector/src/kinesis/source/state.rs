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

use bytes::Bytes;

use crate::state::SourceState;

#[derive(Debug, Clone)]
pub struct KinesisSplitReaderState {
    pub stream_name: String,
    pub shard_id: String,
    pub sequence_number: String,
}

impl KinesisSplitReaderState {
    pub fn new(stream_name: String, shard_id: String, sequence_number: String) -> Self {
        Self {
            stream_name,
            shard_id,
            sequence_number,
        }
    }
}

impl SourceState for KinesisSplitReaderState {
    fn identifier(&self) -> String {
        [
            self.stream_name.clone(),
            "|".to_string(),
            self.shard_id.clone(),
        ]
        .concat()
    }

    fn encode(&self) -> bytes::Bytes {
        let bytes = self.sequence_number.as_bytes();
        Bytes::copy_from_slice(<&[u8]>::clone(&bytes))
    }

    fn decode(&self, values: bytes::Bytes) -> Self {
        let sequence_number = String::from_utf8(values.to_vec()).unwrap();
        Self::new(
            self.stream_name.clone(),
            self.shard_id.clone(),
            sequence_number,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_state_encode() {
        let mock_state = KinesisSplitReaderState::new(
            "mock_stream".to_string(),
            "mock_shard".to_string(),
            "1234567890987654321".to_string(),
        );
        let sequence_number = mock_state.sequence_number.clone();
        let encoded = mock_state.encode();
        let restored = mock_state.decode(encoded);
        assert_eq!(sequence_number, restored.sequence_number);
    }
}
