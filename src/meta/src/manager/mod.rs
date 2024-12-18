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

pub mod diagnose;
mod env;
pub mod event_log;
mod idle;
mod license;
mod metadata;
mod notification;
mod notification_version;
pub mod sink_coordination;
mod streaming_job;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

pub use env::{MetaSrvEnv, *};
pub use event_log::EventLogManagerRef;
pub use idle::*;
pub use metadata::*;
pub use notification::{LocalNotification, MessageStatus, NotificationManagerRef, *};
pub use risingwave_meta_model::prelude;
use risingwave_pb::catalog::{PbSink, PbSource};
use risingwave_pb::common::PbHostAddress;
pub use streaming_job::*;

use crate::MetaResult;

#[derive(Clone, Debug)]
pub struct WorkerKey(pub PbHostAddress);

impl PartialEq<Self> for WorkerKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for WorkerKey {}

impl Hash for WorkerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.host.hash(state);
        self.0.port.hash(state);
    }
}

/// The id preserved for the meta node. Note that there's no such entry in cluster manager.
pub const META_NODE_ID: u32 = 0;

pub fn get_referred_secret_ids_from_source(source: &PbSource) -> MetaResult<HashSet<u32>> {
    let mut secret_ids = HashSet::new();
    for secret_ref in source.get_secret_refs().values() {
        secret_ids.insert(secret_ref.secret_id);
    }
    // `info` must exist in `Source`
    for secret_ref in source.get_info()?.get_format_encode_secret_refs().values() {
        secret_ids.insert(secret_ref.secret_id);
    }
    Ok(secret_ids)
}

pub fn get_referred_connection_ids_from_source(source: &PbSource) -> HashSet<u32> {
    let mut connection_ids = HashSet::new();
    if let Some(conn_id) = source.connection_id {
        connection_ids.insert(conn_id);
    }
    if let Some(info) = &source.info
        && let Some(conn_id) = info.connection_id
    {
        connection_ids.insert(conn_id);
    }
    connection_ids
}

pub fn get_referred_connection_ids_from_sink(sink: &PbSink) -> HashSet<u32> {
    let mut connection_ids = HashSet::new();
    if let Some(format_desc) = &sink.format_desc
        && let Some(conn_id) = format_desc.connection_id
    {
        connection_ids.insert(conn_id);
    }
    if let Some(conn_id) = sink.connection_id {
        connection_ids.insert(conn_id);
    }
    connection_ids
}

pub fn get_referred_secret_ids_from_sink(sink: &PbSink) -> HashSet<u32> {
    let mut secret_ids = HashSet::new();
    for secret_ref in sink.get_secret_refs().values() {
        secret_ids.insert(secret_ref.secret_id);
    }
    // `format_desc` may not exist in `Sink`
    if let Some(format_desc) = &sink.format_desc {
        for secret_ref in format_desc.get_secret_refs().values() {
            secret_ids.insert(secret_ref.secret_id);
        }
    }
    secret_ids
}
