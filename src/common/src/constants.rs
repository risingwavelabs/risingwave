// Copyright 2024 RisingWave Labs
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

pub mod hummock {

    use bitflags::bitflags;
    bitflags! {

        #[derive(Default)]
        pub struct CompactionFilterFlag: u32 {
            const NONE = 0b00000000;
            const STATE_CLEAN = 0b00000010;
            const TTL = 0b00000100;
        }
    }

    impl From<CompactionFilterFlag> for u32 {
        fn from(flag: CompactionFilterFlag) -> Self {
            flag.bits()
        }
    }

    pub const TABLE_OPTION_DUMMY_RETENTION_SECOND: u32 = 0;
    pub const PROPERTIES_RETENTION_SECOND_KEY: &str = "retention_seconds";
}

pub mod log_store {
    use crate::hash::VirtualNode;
    use crate::types::{DataType, ScalarImpl};
    use crate::util::sort_util::OrderType;

    pub type SeqIdType = i32;
    pub type RowOpCodeType = i16;

    pub const EPOCH_COLUMN_NAME: &str = "kv_log_store_epoch";
    pub const SEQ_ID_COLUMN_NAME: &str = "kv_log_store_seq_id";
    pub const ROW_OP_COLUMN_NAME: &str = "kv_log_store_row_op";

    /// Predefined column includes log store pk columns + `row_op`
    pub const KV_LOG_STORE_PREDEFINED_EXTRA_NON_PK_COLUMNS: [(&str, DataType); 1] =
        [(ROW_OP_COLUMN_NAME, ROW_OP_COLUMN_TYPE)];

    pub const EPOCH_COLUMN_TYPE: DataType = DataType::Int64;
    pub const SEQ_ID_COLUMN_TYPE: DataType = DataType::Int32;
    pub const ROW_OP_COLUMN_TYPE: DataType = DataType::Int16;

    pub trait KvLogStorePk: Clone + Send + Sync + Unpin + 'static {
        const LEN: usize;
        const EPOCH_COLUMN_INDEX: usize;
        const ROW_OP_COLUMN_INDEX: usize;
        const SEQ_ID_COLUMN_INDEX: usize;

        fn pk_types() -> [DataType; Self::LEN];
        fn pk_names() -> [&'static str; Self::LEN];
        fn pk_ordering() -> [OrderType; Self::LEN];
        fn pk(
            vnode: VirtualNode,
            encoded_epoch: i64,
            seq_id: Option<SeqIdType>,
        ) -> [Option<ScalarImpl>; Self::LEN];

        fn predefined_column_len() -> usize {
            Self::LEN + KV_LOG_STORE_PREDEFINED_EXTRA_NON_PK_COLUMNS.len()
        }
    }

    pub mod v1 {
        use super::*;

        const EPOCH_COLUMN_INDEX: usize = 0;
        const SEQ_ID_COLUMN_INDEX: usize = 1;
        const ROW_OP_COLUMN_INDEX: usize = 2;

        /// `epoch`, `seq_id`
        const PK_TYPES: [DataType; 2] = [EPOCH_COLUMN_TYPE, SEQ_ID_COLUMN_TYPE];
        const PK_COLUMN_NAMES: [&str; 2] = [EPOCH_COLUMN_NAME, SEQ_ID_COLUMN_NAME];

        #[derive(Clone)]
        pub struct KvLogStoreV1Pk;

        impl KvLogStorePk for KvLogStoreV1Pk {
            const EPOCH_COLUMN_INDEX: usize = EPOCH_COLUMN_INDEX;
            const LEN: usize = PK_TYPES.len();
            const ROW_OP_COLUMN_INDEX: usize = ROW_OP_COLUMN_INDEX;
            const SEQ_ID_COLUMN_INDEX: usize = SEQ_ID_COLUMN_INDEX;

            fn pk_types() -> [DataType; Self::LEN] {
                PK_TYPES
            }

            fn pk_names() -> [&'static str; Self::LEN] {
                PK_COLUMN_NAMES
            }

            fn pk_ordering() -> [OrderType; Self::LEN] {
                [OrderType::ascending(), OrderType::ascending_nulls_last()]
            }

            fn pk(
                _vnode: VirtualNode,
                encoded_epoch: i64,
                seq_id: Option<SeqIdType>,
            ) -> [Option<ScalarImpl>; Self::LEN] {
                [
                    Some(ScalarImpl::Int64(encoded_epoch)),
                    seq_id.map(ScalarImpl::Int32),
                ]
            }
        }
    }
}
