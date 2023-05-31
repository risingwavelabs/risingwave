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

use rand::Rng;

use super::TxnId;
use crate::array::StreamChunk;

pub enum TxnMsg {
    Begin(TxnId),
    Data(TxnId, StreamChunk),
    End(TxnId),
}

impl TxnMsg {
    pub fn txn_id(&self) -> TxnId {
        match self {
            Self::Begin(txn_id) => *txn_id,
            Self::Data(txn_id, _) => *txn_id,
            Self::End(txn_id) => *txn_id,
        }
    }
}

pub fn generate_txn_id() -> TxnId {
    let mut rng = rand::thread_rng();
    rng.gen()
}
