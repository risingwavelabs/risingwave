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

use async_trait::async_trait;
use risingwave_common::system_param::{system_params_from_kv, system_params_to_kv};
use risingwave_pb::meta::SystemParams;

use crate::model::{MetadataModelError, MetadataModelResult, Transactional};
use crate::storage::{MetaStore, Snapshot, Transaction};

const SYSTEM_PARAMS_CF_NAME: &str = "cf/system_params";

// A dummy trait to implement custom methods on `SystemParams`.
#[async_trait]
pub trait SystemParamsModel: Sized {
    fn cf_name() -> String;
    async fn get<S: MetaStore>(store: &S) -> MetadataModelResult<Option<Self>>;
    async fn get_at_snapshot<S: MetaStore>(
        store: &S::Snapshot,
    ) -> MetadataModelResult<Option<Self>>;
    async fn insert<S: MetaStore>(&self, store: &S) -> MetadataModelResult<()>;
}

#[async_trait]
impl SystemParamsModel for SystemParams {
    fn cf_name() -> String {
        SYSTEM_PARAMS_CF_NAME.to_string()
    }

    /// Return error if there are missing or unrecognized fields.
    async fn get<S>(store: &S) -> MetadataModelResult<Option<Self>>
    where
        S: MetaStore,
    {
        Self::get_at_snapshot::<S>(&store.snapshot().await).await
    }

    async fn get_at_snapshot<S>(snapshot: &S::Snapshot) -> MetadataModelResult<Option<SystemParams>>
    where
        S: MetaStore,
    {
        let kvs = snapshot.list_cf(&SystemParams::cf_name()).await?;
        if kvs.is_empty() {
            Ok(None)
        } else {
            Ok(Some(
                system_params_from_kv(kvs).map_err(MetadataModelError::internal)?,
            ))
        }
    }

    /// All undeprecated fields must be `Some`.
    /// Return error if there are missing fields.
    async fn insert<S>(&self, store: &S) -> MetadataModelResult<()>
    where
        S: MetaStore,
    {
        let mut txn = Transaction::default();
        self.upsert_in_transaction(&mut txn)?;
        Ok(store.txn(txn).await?)
    }
}

impl Transactional for SystemParams {
    fn upsert_in_transaction(&self, trx: &mut Transaction) -> MetadataModelResult<()> {
        for (k, v) in system_params_to_kv(self).map_err(MetadataModelError::internal)? {
            trx.put(Self::cf_name(), k.into_bytes(), v.into_bytes());
        }
        Ok(())
    }

    fn delete_in_transaction(&self, _trx: &mut Transaction) -> MetadataModelResult<()> {
        unreachable!()
    }
}
