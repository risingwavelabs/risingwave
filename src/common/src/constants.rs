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
