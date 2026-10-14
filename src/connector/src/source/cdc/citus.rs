// Copyright 2026 RisingWave Labs
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

use std::collections::BTreeMap;

use risingwave_pb::plan_common::{PbEncodeType, PbFormatType};

use crate::error::ConnectorResult;
use crate::source::cdc::postgres::{PostgresCdcSplit, fill_and_validate_postgres_props};
use crate::source::cdc::split::DebeziumCdcSplit;
use crate::source::cdc::{CdcSerializedFieldNames, CdcSourceFrontendValidator, Citus, Postgres};

// ------- Persisted names -------

impl CdcSerializedFieldNames for Citus {}

// ------- Frontend validation -------

impl CdcSourceFrontendValidator for Citus {
    const ALLOW_ALTER_ON_FLY_FIELDS: &'static [&'static str] = &[];
    const COMPATIBLE_FORMATS: &'static [(PbFormatType, &'static [PbEncodeType])] =
        Postgres::COMPATIBLE_FORMATS;

    fn fill_and_validate_props(props: &mut BTreeMap<String, String>) -> ConnectorResult<()> {
        fill_and_validate_postgres_props(props)
    }
}

// ------- Split -------

/// Citus is a Postgres extension and shares the Postgres split.
pub type CitusCdcSplit = PostgresCdcSplit;

impl DebeziumCdcSplit<Citus> {
    /// Citus has one split per worker node; `server_addr` is that node's host and port.
    pub fn new_with_server_addr(split_id: u32, server_addr: String) -> Self {
        let mut split = Self::new(split_id, None);
        split
            .citus_split
            .as_mut()
            .expect("citus split must exist")
            .server_addr = Some(server_addr);
        split
    }
}
