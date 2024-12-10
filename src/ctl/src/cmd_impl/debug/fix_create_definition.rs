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

use etcd_client::ConnectOptions;
use risingwave_meta::model::MetadataModel;
use risingwave_meta::storage::{EtcdMetaStore, WrappedEtcdClient};
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::catalog::{PbSource, PbTable};

use crate::DebugCommon;

pub async fn fix_create_definition(common: DebugCommon, dry_run: bool) -> anyhow::Result<()> {
    let DebugCommon {
        etcd_endpoints,
        etcd_username,
        etcd_password,
        enable_etcd_auth,
        ..
    } = common;

    let client = if enable_etcd_auth {
        let options = ConnectOptions::default().with_user(
            etcd_username.clone().unwrap_or_default(),
            etcd_password.clone().unwrap_or_default(),
        );
        WrappedEtcdClient::connect(etcd_endpoints.clone(), Some(options), true).await?
    } else {
        WrappedEtcdClient::connect(etcd_endpoints.clone(), None, false).await?
    };

    let meta_store = EtcdMetaStore::new(client);

    let mut tables = PbTable::list(&meta_store).await?;
    for table in &mut tables {
        if table.table_type() != PbTableType::Internal
            && table.table_type() != PbTableType::Index
            && (table.definition.contains("*WATERMARK") || table.definition.contains(")WATERMARK"))
        {
            println!("table: id={}, name={}: ", table.id, table.name);
            let new_definition = table
                .definition
                .replace("*WATERMARK", "*, WATERMARK")
                .replace(")WATERMARK", "), WATERMARK");
            println!("\told definition: {}", table.definition);
            println!("\tnew definition: {}", new_definition);
            if !dry_run {
                table.definition = new_definition;
                table.insert(&meta_store).await?;
                println!("table definition updated");
            }
        }
    }
    let mut sources = PbSource::list(&meta_store).await?;
    for source in &mut sources {
        if source.definition.contains("*WATERMARK") || source.definition.contains(")WATERMARK") {
            println!("source: id={}, name={}: ", source.id, source.name);
            let new_definition = source
                .definition
                .replace("*WATERMARK", "*, WATERMARK")
                .replace(")WATERMARK", "), WATERMARK");
            println!("\told definition: {}", source.definition);
            println!("\tnew definition: {}", new_definition);
            if !dry_run {
                source.definition = new_definition;
                source.insert(&meta_store).await?;
                println!("source definition updated");
            }
        }
    }

    Ok(())
}
