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

mod resize;

pub use resize::*;
use risingwave_pb::meta::table_parallelism::FixedParallelism;
use risingwave_pb::meta::{TableParallelism, table_parallelism};

use crate::common::CtlContext;

pub async fn set_cdc_table_backfill_parallelism(
    context: &CtlContext,
    table_id: u32,
    parallelism: u32,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    meta_client
        .alter_cdc_table_backfill_parallelism(
            table_id,
            TableParallelism {
                parallelism: Some(table_parallelism::Parallelism::Fixed(FixedParallelism {
                    parallelism,
                })),
            },
        )
        .await?;
    Ok(())
}
