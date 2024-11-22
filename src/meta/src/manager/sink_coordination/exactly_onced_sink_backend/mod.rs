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

mod iceberg;

use risingwave_connector::sink::file_sink::opendal_sink::{FileSink, OpendalSinkBackend};
use risingwave_connector::sink::iceberg::IcebergSink;
use risingwave_connector::sink::{SinkError, SinkParam};
use risingwave_pb::connector_service::SinkCoordinatorPreCommitMetadata;

pub trait HandlePreCommit {
    async fn persist_pre_commit_metadata(
        &self,
        pre_commit_metadata: SinkCoordinatorPreCommitMetadata,
    ) -> anyhow::Result<()>;

    fn clear(&mut self) -> anyhow::Result<()>;

    // Used in recovery
    // for file sink, query_s3 and rewind log store
    // for others, re commit
    fn re_commit(&self) -> anyhow::Result<()>;
}

pub enum SinkBackend {
    Iceberg(IcebergSink),
}

impl SinkBackend {
    pub fn new(sink_param: SinkParam) -> Result<Self, SinkError> {
        match sink_param.properties.get("connector").map(String::as_str) {
            Some("iceberg") => {
                let iceberg_sink = IcebergSink::try_from(sink_param)?;
                Ok(SinkBackend::Iceberg(iceberg_sink))
            }
            Some(&_) => todo!(),
            None => unreachable!(),
        }
    }
}

impl HandlePreCommit for SinkBackend {
    async fn persist_pre_commit_metadata(
        &self,
        pre_commit_metadata: SinkCoordinatorPreCommitMetadata,
    ) -> anyhow::Result<()> {
        match self {
            SinkBackend::Iceberg(iceberg_sink) => {
                iceberg_sink
                    .persist_pre_commit_metadata(pre_commit_metadata)
                    .await
            }
        }
    }

    fn clear(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn re_commit(&self) -> anyhow::Result<()> {
        todo!()
    }
}
