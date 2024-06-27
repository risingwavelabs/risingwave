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

use risingwave_common::bail;
use risingwave_pb::catalog::{Sink, Source};

use crate::manager::{ConnectionId, DatabaseManager};
use crate::MetaResult;

pub fn refcnt_inc_connection(
    database_mgr: &mut DatabaseManager,
    connection_id: Option<ConnectionId>,
) -> anyhow::Result<()> {
    if let Some(connection_id) = connection_id {
        if let Some(_conn) = database_mgr.get_connection(connection_id) {
            database_mgr.increase_connection_ref_count(connection_id);
        } else {
            bail!("connection {} not found.", connection_id);
        }
    }
    Ok(())
}

pub fn refcnt_dec_connection(
    database_mgr: &mut DatabaseManager,
    connection_id: Option<ConnectionId>,
) {
    if let Some(connection_id) = connection_id {
        database_mgr.decrease_connection_ref_count(connection_id);
    }
}

pub fn refcnt_inc_source_secret_ref(
    database_mgr: &mut DatabaseManager,
    source: &Source,
) -> MetaResult<()> {
    for secret_ref in source.get_secret_refs().values() {
        database_mgr.increase_secret_ref_count(secret_ref.secret_id)
    }
    for secret_ref in source.get_info()?.get_format_encode_secret_refs().values() {
        database_mgr.increase_secret_ref_count(secret_ref.secret_id)
    }
    Ok(())
}

pub fn refcnt_dec_source_secret_ref(
    database_mgr: &mut DatabaseManager,
    source: &Source,
) -> MetaResult<()> {
    for secret_ref in source.get_secret_refs().values() {
        database_mgr.decrease_secret_ref_count(secret_ref.secret_id)
    }
    for secret_ref in source.get_info()?.get_format_encode_secret_refs().values() {
        database_mgr.decrease_secret_ref_count(secret_ref.secret_id)
    }
    Ok(())
}

pub fn refcnt_inc_sink_secret_ref(database_mgr: &mut DatabaseManager, sink: &Sink) {
    for secret_ref in sink.get_secret_refs().values() {
        database_mgr.increase_secret_ref_count(secret_ref.secret_id)
    }
    if let Some(format_desc) = &sink.format_desc {
        for secret_ref in format_desc.get_secret_refs().values() {
            database_mgr.increase_secret_ref_count(secret_ref.secret_id)
        }
    }
}

pub fn refcnt_dec_sink_secret_ref(database_mgr: &mut DatabaseManager, sink: &Sink) {
    for secret_ref in sink.get_secret_refs().values() {
        database_mgr.decrease_secret_ref_count(secret_ref.secret_id)
    }
    if let Some(format_desc) = &sink.format_desc {
        for secret_ref in format_desc.get_secret_refs().values() {
            database_mgr.decrease_secret_ref_count(secret_ref.secret_id)
        }
    }
}
