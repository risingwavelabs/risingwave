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

pub mod base;
pub mod cdc;
pub mod data_gen_util;
pub mod datagen;
pub mod dummy_connector;
pub mod filesystem;
pub mod google_pubsub;
pub mod kafka;
pub mod kinesis;
pub mod monitor;
pub mod nexmark;
pub mod pulsar;
pub use base::*;
pub use google_pubsub::GOOGLE_PUBSUB_CONNECTOR;
pub use kafka::KAFKA_CONNECTOR;
pub use kinesis::KINESIS_CONNECTOR;
mod manager;
pub use manager::SourceColumnDesc;

pub use crate::source::nexmark::NEXMARK_CONNECTOR;
pub use crate::source::pulsar::PULSAR_CONNECTOR;
