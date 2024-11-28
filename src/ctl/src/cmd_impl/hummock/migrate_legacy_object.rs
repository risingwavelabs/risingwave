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

use anyhow::anyhow;
use futures::StreamExt;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_hummock_sdk::{get_object_id_from_path, get_sst_data_path, OBJECT_SUFFIX};
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::prefix::opendal_engine::get_object_prefix;
use risingwave_object_store::object::{build_remote_object_store, ObjectStoreImpl};

pub async fn migrate_legacy_object(
    url: String,
    source_dir: String,
    target_dir: String,
) -> anyhow::Result<()> {
    let source_dir = source_dir.trim_end_matches('/');
    let target_dir = target_dir.trim_end_matches('/');
    println!("Normalized source_dir: {source_dir}.");
    println!("Normalized target_dir: {target_dir}.");
    if source_dir.is_empty() || target_dir.is_empty() {
        return Err(anyhow!("the source_dir and target_dir must not be empty"));
    }
    if target_dir.starts_with(&source_dir) {
        return Err(anyhow!("the target_dir must not include source_dir"));
    }
    let mut config = ObjectStoreConfig::default();
    config.s3.developer.use_opendal = true;
    let store = build_remote_object_store(
        &url,
        ObjectStoreMetrics::unused().into(),
        "migrate_legacy_object",
        config.into(),
    )
    .await;
    let ObjectStoreImpl::Opendal(opendal) = store else {
        return Err(anyhow!("OpenDAL is required"));
    };
    let mut iter = opendal.list(&source_dir, None, None).await?;
    let mut count = 0;
    println!("Migration is started: from {source_dir} to {target_dir}.");
    while let Some(object) = iter.next().await {
        let object = object?;
        if !object.key.ends_with(OBJECT_SUFFIX) {
            let legacy_path = object.key;
            assert_eq!(
                &legacy_path[..source_dir.len()],
                source_dir,
                "{legacy_path} versus {source_dir}"
            );
            let new_path = format!("{}{}", target_dir, &legacy_path[source_dir.len()..]);
            println!("From {legacy_path} to {new_path}");
            opendal.inner().copy(&legacy_path, &new_path).await?;
            count += 1;
            continue;
        }
        let object_id = get_object_id_from_path(&object.key);
        let legacy_prefix = get_object_prefix(object_id, false);
        let legacy_path = get_sst_data_path(&legacy_prefix, &source_dir, object_id);
        if object.key != legacy_path {
            return Err(anyhow!(format!(
                "the source object store does not appear to be legacy: {} versus {}",
                object.key, legacy_path
            )));
        }
        let new_path =
            get_sst_data_path(&get_object_prefix(object_id, true), &target_dir, object_id);
        println!("From {legacy_path} to {new_path}.");
        opendal.inner().copy(&legacy_path, &new_path).await?;
        count += 1;
    }
    println!("Migration is finished. {count} objects have been migrated from {source_dir} to {target_dir}.");
    Ok(())
}
