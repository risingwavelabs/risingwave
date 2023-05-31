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

use std::fmt;

use rand::Rng;

use super::TxnId;
use crate::array::StreamChunk;
use crate::transaction::transaction_message::TxnMsg::{Begin, Data, End};

pub enum TxnMsg {
    Begin(TxnId),
    Data(TxnId, StreamChunk),
    End(TxnId),
}

impl TxnMsg {
    pub fn txn_id(&self) -> TxnId {
        match self {
            Begin(txn_id) => *txn_id,
            Data(txn_id, _) => *txn_id,
            End(txn_id) => *txn_id,
        }
    }

    pub fn as_stream_chunk(&self) -> Option<StreamChunk> {
        match self {
            Begin(_) | Self::End(_) => None,
            Data(_, chunk) => Some(chunk.clone()),
        }
    }
}

impl fmt::Debug for TxnMsg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Begin(txn_id) => write!(f, "Begin {{ txn_id: {} }}", txn_id,),
            Data(txn_id, chunk) => {
                write!(f, "Data {{ txn_id: {}, chunk: \n{:?}\n }}", txn_id, chunk,)
            }
            End(txn_id) => write!(f, "End {{ txn_id: {} }}", txn_id,),
        }
    }
}

pub fn generate_txn_id() -> TxnId {
    // TODO: generate txn id in a better way.
    let mut rng = rand::thread_rng();
    rng.gen()
}
