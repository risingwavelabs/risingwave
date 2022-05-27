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

use bytes::{Buf, BufMut, BytesMut};
use risingwave_hummock_sdk::key::next_key;
use risingwave_storage::StateStore;

use crate::common::HummockServiceOpts;

pub async fn list_kv(epoch: u64, table_id: u32) -> anyhow::Result<()> {
    let hummock_opts = HummockServiceOpts::from_env()?;
    let hummock = hummock_opts.create_hummock_store().await?;
    if epoch == u64::MAX {
        tracing::info!("using u64::MAX as epoch");
    }
    let scan_result = match table_id {
        u32::MAX => {
            tracing::info!("using .. as range");
            hummock
                .scan::<_, Vec<u8>>(.., None, u64::MAX, vec![])
                .await?
        }
        _ => {
            let mut buf = BytesMut::with_capacity(5);
            buf.put_u8(b't');
            buf.put_u32(table_id);
            let range = buf.to_vec()..next_key(buf.to_vec().as_slice());
            hummock
                .scan::<_, Vec<u8>>(range, None, u64::MAX, vec![])
                .await?
        }
    };
    for (k, v) in scan_result {
        let print_string = match k[0] {
            b't' => {
                let mut buf = &k[1..];
                format!("table_id:{:?}", buf.get_u32())
            }
            b's' => {
                let mut buf = &k[1..];
                format!("shared_executor_id:{:?}", buf.get_u64())
            }
            b'e' => {
                let mut buf = &k[1..];
                format!("executor_id:{:?}", buf.get_u64())
            }
            _ => "no title".to_string(),
        };
        println!("{}\n key : {:?} ====> value : {:?}", print_string, k, v)
    }

    Ok(())
}
