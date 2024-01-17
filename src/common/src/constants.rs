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
    use tinyvec::ArrayVec;

    use crate::hash::VirtualNode;
    use crate::types::{DataType, Datum, ScalarImpl};
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

    pub type KvLogStorePkRow = ArrayVec<[Datum; 3]>;

    pub type ComputePkFn =
        fn(vnode: VirtualNode, encoded_epoch: i64, seq_id: Option<SeqIdType>) -> KvLogStorePkRow;

    pub struct KvLogStorePkInfo {
        pub len: usize,
        pub epoch_column_index: usize,
        pub row_op_column_index: usize,
        pub seq_id_column_index: usize,
        pub types: &'static [DataType],
        pub names: &'static [&'static str],
        pub orderings: &'static [OrderType],
        pub compute_pk: ComputePkFn,
    }

    impl KvLogStorePkInfo {
        pub fn predefined_column_len(&self) -> usize {
            self.len + KV_LOG_STORE_PREDEFINED_EXTRA_NON_PK_COLUMNS.len()
        }
    }

    pub mod v1 {
        use std::sync::LazyLock;

        use super::*;

        const EPOCH_COLUMN_INDEX: usize = 0;
        const SEQ_ID_COLUMN_INDEX: usize = 1;
        const ROW_OP_COLUMN_INDEX: usize = 2;

        /// `epoch`, `seq_id`
        const PK_TYPES: [DataType; 2] = [EPOCH_COLUMN_TYPE, SEQ_ID_COLUMN_TYPE];
        const PK_COLUMN_NAMES: [&str; 2] = [EPOCH_COLUMN_NAME, SEQ_ID_COLUMN_NAME];

        pub static KV_LOG_STORE_V1_INFO: LazyLock<KvLogStorePkInfo> = LazyLock::new(|| {
            static PK_ORDERING: [OrderType; PK_TYPES.len()] =
                [OrderType::ascending(), OrderType::ascending_nulls_last()];
            fn compute_pk(
                _vnode: VirtualNode,
                encoded_epoch: i64,
                seq_id: Option<SeqIdType>,
            ) -> KvLogStorePkRow {
                KvLogStorePkRow::from_array_len(
                    [
                        Some(ScalarImpl::Int64(encoded_epoch)),
                        seq_id.map(ScalarImpl::Int32),
                        None,
                    ],
                    2,
                )
            }
            KvLogStorePkInfo {
                len: 0,
                epoch_column_index: EPOCH_COLUMN_INDEX,
                row_op_column_index: ROW_OP_COLUMN_INDEX,
                seq_id_column_index: SEQ_ID_COLUMN_INDEX,
                types: &PK_TYPES[..],
                names: &PK_COLUMN_NAMES[..],
                orderings: &PK_ORDERING[..],
                compute_pk,
            }
        });
    }
}
