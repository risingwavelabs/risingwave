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

pub mod enumerator;
pub mod source;
pub mod split;

use std::collections::HashMap;

use anyhow::anyhow;
use risingwave_pb::connector_service::TableSchema;
use serde::Deserialize;

use crate::common::KafkaCommon;
pub const NATS_CONNECTOR: &str = "nats";

#[derive(Clone, Debug, Deserialize)]
pub struct NatsProperties {
    /// Properties specified in the WITH clause by user
    pub props: HashMap<String, String>,

    /// Schema of the source specified by users
    pub table_schema: Option<TableSchema>,
}

impl NatsProperties {}
