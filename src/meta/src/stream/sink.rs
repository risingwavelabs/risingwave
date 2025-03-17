// Copyright 2025 RisingWave Labs
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

use anyhow::Context;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_connector::sink::{Sink, SinkParam, build_sink};
use risingwave_pb::catalog::PbSink;

use crate::MetaResult;

pub async fn validate_sink(prost_sink_catalog: &PbSink) -> MetaResult<()> {
    let sink_catalog = SinkCatalog::from(prost_sink_catalog);
    let param = SinkParam::try_from_sink_catalog(sink_catalog)?;

    let sink = build_sink(param)?;

    dispatch_sink!(
        sink,
        sink,
        Ok(sink.validate().await.context("failed to validate sink")?)
    )
}
