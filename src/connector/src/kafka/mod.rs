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

use std::time::Duration;

pub(crate) mod enumerator;
pub mod source;
mod split;

pub use enumerator::*;
pub use source::*;
pub use split::*;

const KAFKA_SYNC_CALL_TIMEOUT: Duration = Duration::from_secs(1);

const KAFKA_CONFIG_BROKER_KEY: &str = "kafka.broker";
const KAFKA_CONFIG_TOPIC_KEY: &str = "kafka.topic";
const KAFKA_CONFIG_SCAN_STARTUP_MODE: &str = "kafka.scan.startup.mode";
const KAFKA_CONFIG_TIME_OFFSET: &str = "kafka.time.offset";
const KAFKA_CONFIG_CONSUME_GROUP: &str = "kafka.consumer.group";
