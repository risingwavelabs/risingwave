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

use icelake::types::{data_file_from_json, data_file_to_json, Any, DataFile};
use risingwave_connector::sink::iceberg::{IcebergSink, WriteResult};
use risingwave_connector::sink::SinkParam;

use super::HandlePreCommit;

impl HandlePreCommit for IcebergSink {
    async fn persist_pre_commit_metadata(
        &self,
        pre_commit_metadata: risingwave_pb::connector_service::SinkCoordinatorPreCommitMetadata,
    ) -> anyhow::Result<()> {
        let table = self.create_and_validate_table().await?;
        let partition_type = table.current_partition_type()?;
        let write_results =
            WriteResult::try_from_pre_commit_metadata(&pre_commit_metadata, &partition_type)?;

        Ok(())
    }

    fn clear(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn re_commit(&self) -> anyhow::Result<()> {
        todo!()
    }
}
