// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;

use bytes::{Buf, BufMut, BytesMut};
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::next_key;
use risingwave_storage::store::ReadOptions;
use risingwave_storage::StateStore;

use crate::common::HummockServiceOpts;

pub async fn list_kv(epoch: u64, table_id: u32) -> anyhow::Result<()> {
    let mut hummock_opts = HummockServiceOpts::from_env()?;
    let (_meta_client, hummock) = hummock_opts.create_hummock_store().await?;
    if epoch == u64::MAX {
        tracing::info!("using u64::MAX as epoch");
    }
    let scan_result = {
        let mut buf = BytesMut::with_capacity(5);
        buf.put_u32(table_id);
        let range = (
            Bound::Included(buf.to_vec()),
            Bound::Excluded(next_key(buf.to_vec().as_slice())),
        );
        hummock
            .scan(
                None,
                range,
                None,
                ReadOptions {
                    epoch,
                    table_id: TableId { table_id },
                    retention_seconds: None,
                },
            )
            .await?
    };
    for (k, v) in scan_result {
        let mut buf = &k[..];
        let print_string = format!("[t{}]", buf.get_u32());
        println!("{} {:?} => {:?}", print_string, k, v)
    }
    hummock_opts.shutdown().await;
    Ok(())
}
