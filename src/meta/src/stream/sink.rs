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

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_connector::sink::{build_sink, Sink, SinkConfig};
use risingwave_pb::catalog::PbSink;
use risingwave_rpc_client::ConnectorClient;

use crate::{MetaError, MetaResult};

pub async fn validate_sink(
    prost_sink_catalog: &PbSink,
    connector_client: Option<ConnectorClient>,
) -> MetaResult<()> {
    let sink_catalog = SinkCatalog::from(prost_sink_catalog);
    let properties = sink_catalog.properties.clone();
    let sink_config = SinkConfig::from_hashmap(properties)
        .map_err(|err| MetaError::from(anyhow!(err.to_string())))?;

    let sink = build_sink(
        sink_config,
        &sink_catalog.visible_columns().cloned().collect_vec(),
        sink_catalog.downstream_pk_indices(),
        sink_catalog.sink_type,
        sink_catalog.id,
    )?;

    dispatch_sink!(sink, sink, { Ok(sink.validate(connector_client).await?) })
}
