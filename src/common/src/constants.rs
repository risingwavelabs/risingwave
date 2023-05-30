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

pub mod hummock {
    #![expect(clippy::disallowed_methods, reason = "used by bitflags!")]

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
    use crate::types::DataType;

    pub const EPOCH_COLUMN_NAME: &str = "kv_log_store_epoch";
    pub const SEQ_ID_COLUMN_NAME: &str = "kv_log_store_seq_id";
    pub const ROW_OP_COLUMN_NAME: &str = "kv_log_store_row_op";

    pub const EPOCH_COLUMN_TYPE: DataType = DataType::Int64;
    pub const SEQ_ID_COLUMN_TYPE: DataType = DataType::Int32;
    pub const ROW_OP_COLUMN_TYPE: DataType = DataType::Int16;

    pub const EPOCH_COLUMN_INDEX: usize = 0;
    pub const SEQ_ID_COLUMN_INDEX: usize = 1;
    pub const ROW_OP_COLUMN_INDEX: usize = 2;
}
