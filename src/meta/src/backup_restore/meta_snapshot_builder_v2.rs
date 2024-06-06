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

use std::future::Future;

use itertools::Itertools;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};
use risingwave_backup::MetaSnapshotId;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_meta_model_v2 as model_v2;
use risingwave_pb::hummock::PbHummockVersionDelta;
use sea_orm::{DbErr, EntityTrait, QueryOrder, TransactionTrait};

use crate::controller::SqlMetaStore;

const VERSION: u32 = 2;

fn map_db_err(e: DbErr) -> BackupError {
    BackupError::MetaStorage(e.into())
}

pub struct MetaSnapshotV2Builder {
    snapshot: MetaSnapshotV2,
    meta_store: SqlMetaStore,
}

impl MetaSnapshotV2Builder {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self {
            snapshot: MetaSnapshotV2::default(),
            meta_store,
        }
    }

    pub async fn build(
        &mut self,
        id: MetaSnapshotId,
        hummock_version_builder: impl Future<Output = HummockVersion>,
    ) -> BackupResult<()> {
        self.snapshot.format_version = VERSION;
        self.snapshot.id = id;
        // Get `hummock_version` before `meta_store_snapshot`.
        // We have ensure the required delta logs for replay is available, see
        // `HummockManager::delete_version_deltas`.
        let hummock_version = hummock_version_builder.await;
        let txn = self
            .meta_store
            .conn
            .begin_with_config(
                Some(sea_orm::IsolationLevel::Serializable),
                Some(sea_orm::AccessMode::ReadOnly),
            )
            .await
            .map_err(map_db_err)?;
        let version_deltas = model_v2::prelude::HummockVersionDelta::find()
            .order_by_asc(model_v2::hummock_version_delta::Column::Id)
            .all(&txn)
            .await
            .map_err(map_db_err)?
            .into_iter()
            .map_into::<PbHummockVersionDelta>()
            .map(|pb_delta| HummockVersionDelta::from_persisted_protobuf(&pb_delta));
        let hummock_version = {
            let mut redo_state = hummock_version;
            let mut max_log_id = None;
            for version_delta in version_deltas {
                if version_delta.prev_id == redo_state.id {
                    redo_state.apply_version_delta(&version_delta);
                }
                max_log_id = Some(version_delta.id);
            }
            if let Some(max_log_id) = max_log_id {
                if max_log_id != redo_state.id {
                    return Err(BackupError::Other(anyhow::anyhow!(format!(
                        "inconsistent hummock version: expected {}, actual {}",
                        max_log_id, redo_state.id
                    ))));
                }
            }
            redo_state
        };
        let version_stats = model_v2::prelude::HummockVersionStats::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let compaction_configs = model_v2::prelude::CompactionConfig::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let actors = model_v2::prelude::Actor::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let clusters = model_v2::prelude::Cluster::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let actor_dispatchers = model_v2::prelude::ActorDispatcher::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let catalog_versions = model_v2::prelude::CatalogVersion::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let connections = model_v2::prelude::Connection::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let databases = model_v2::prelude::Database::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let fragments = model_v2::prelude::Fragment::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let functions = model_v2::prelude::Function::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let indexes = model_v2::prelude::Index::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let objects = model_v2::prelude::Object::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let object_dependencies = model_v2::prelude::ObjectDependency::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let schemas = model_v2::prelude::Schema::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let sinks = model_v2::prelude::Sink::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let sources = model_v2::prelude::Source::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let streaming_jobs = model_v2::prelude::StreamingJob::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let subscriptions = model_v2::prelude::Subscription::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let system_parameters = model_v2::prelude::SystemParameter::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let tables = model_v2::prelude::Table::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let users = model_v2::prelude::User::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let user_privileges = model_v2::prelude::UserPrivilege::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let views = model_v2::prelude::View::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let workers = model_v2::prelude::Worker::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let worker_properties = model_v2::prelude::WorkerProperty::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let hummock_sequences = model_v2::prelude::HummockSequence::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let seaql_migrations = model_v2::serde_seaql_migration::Entity::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;
        let session_parameters = model_v2::prelude::SessionParameter::find()
            .all(&txn)
            .await
            .map_err(map_db_err)?;

        txn.commit().await.map_err(map_db_err)?;
        self.snapshot.metadata = MetadataV2 {
            seaql_migrations,
            hummock_version,
            version_stats,
            compaction_configs,
            actors,
            clusters,
            actor_dispatchers,
            catalog_versions,
            connections,
            databases,
            fragments,
            functions,
            indexes,
            objects,
            object_dependencies,
            schemas,
            sinks,
            sources,
            streaming_jobs,
            subscriptions,
            system_parameters,
            tables,
            users,
            user_privileges,
            views,
            workers,
            worker_properties,
            hummock_sequences,
            session_parameters,
        };
        Ok(())
    }

    pub fn finish(self) -> BackupResult<MetaSnapshotV2> {
        // Any sanity check goes here.
        Ok(self.snapshot)
    }
}
