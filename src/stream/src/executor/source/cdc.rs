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

use risingwave_connector::error::ConnectorResult;
use risingwave_connector::source::cdc::external::{
    CdcOffset, ExternalCdcTableType, ExternalDatabaseConfig,
};

pub async fn try_initialize_cdc_source_state(
    config: &ExternalDatabaseConfig,
) -> ConnectorResult<()> {
    let Some(offset) = ExternalCdcTableType::from_str(&config.connector)
        .get_current_cdc_offset(config)
        .await?
    else {
        return Ok(());
    };
    tracing::debug!(current_offste=?offset);
    // TODO
    Ok(())
}
