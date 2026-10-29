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

//! Delete-aware Iceberg ingestion (`streaming_updates = 'true'`).
//!
//! List dispatches bounded pages continuously within a phase. Fetch retires tasks at EOF.
//! Phase transitions wait asynchronously for an empty committed Fetch queue, while ordinary
//! barriers remain responsive. Output and physical cursors recover at the same checkpoint.

mod fetch;
mod list;

use anyhow::Context;
pub use fetch::IcebergUpdateFetchExecutor;
pub use list::IcebergUpdateListExecutor;
use risingwave_common::types::{JsonbVal, ScalarRef};
use risingwave_common::{bail, ensure};
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::{IcebergProperties, IcebergScanMetricsLabels};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::StreamSourceCore;
use crate::executor::prelude::*;

pub(crate) fn validate_update_state_table(
    table: &risingwave_pb::catalog::Table,
    singleton: bool,
) -> StreamExecutorResult<()> {
    use risingwave_pb::data::data_type::TypeName;
    let types = table
        .columns
        .iter()
        .map(|column| {
            column
                .column_desc
                .as_ref()
                .and_then(|desc| desc.column_type.as_ref())
                .map(|ty| ty.type_name())
        })
        .collect::<Vec<_>>();
    if singleton {
        ensure!(
            types == vec![Some(TypeName::Jsonb)]
                && table.pk.is_empty()
                && table.value_indices == vec![0]
                && table.distribution_key.is_empty(),
            "Iceberg update List state requires a single JSON value without a primary key"
        );
    } else {
        ensure!(
            types == vec![Some(TypeName::Varchar), Some(TypeName::Jsonb)]
                && table.pk.len() == 1
                && table.pk[0].column_index == 0
                && table.value_indices == vec![0, 1]
                && table.distribution_key == vec![0],
            "Iceberg update Fetch state requires a varchar task key and JSON value with matching distribution"
        );
    }
    Ok(())
}

fn properties(
    core: &mut StreamSourceCore<impl StateStore>,
) -> StreamExecutorResult<IcebergProperties> {
    let builder = core
        .source_desc_builder
        .take()
        .context("missing Iceberg source description")?;
    let ConnectorProperties::Iceberg(properties) =
        ConnectorProperties::extract(builder.with_properties(), false)?
    else {
        bail!("update source requires Iceberg");
    };
    Ok(*properties)
}

fn scan_metrics(
    core: &StreamSourceCore<impl StateStore>,
    properties: &IcebergProperties,
) -> IcebergScanMetricsLabels {
    IcebergScanMetricsLabels::new(
        core.source_id.to_string(),
        core.source_name.clone(),
        properties.table.table_name().to_owned(),
    )
}

fn encode(value: &impl Serialize) -> StreamExecutorResult<JsonbVal> {
    Ok(serde_json::to_value(value)
        .map_err(anyhow::Error::from)?
        .into())
}

fn decode<T: DeserializeOwned>(row: impl Row) -> StreamExecutorResult<T> {
    let value = row
        .datum_at(1)
        .context("missing Iceberg update state")?
        .into_jsonb();
    decode_value(value.to_owned_scalar())
}

fn decode_value<T: DeserializeOwned>(value: JsonbVal) -> StreamExecutorResult<T> {
    Ok(serde_json::from_value(value.take()).map_err(anyhow::Error::from)?)
}

#[cfg(test)]
mod tests;
