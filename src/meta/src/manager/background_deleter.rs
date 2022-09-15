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

use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_pb::catalog::{Sink, Table};
use tokio::sync::oneshot::Sender;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::manager::MetaSrvEnv;
use crate::model::{MetadataModel, MetadataModelResult};
use crate::storage::MetaStore;
use crate::stream::GlobalStreamManagerRef;
use crate::MetaResult;

pub type TableBackgroundDeleterRef = Arc<TableBackgroundDeleter>;

macro_rules! create_catalog_deleted {
    ($name:ident, $cf:ident) => {
        paste::paste! {
            #[derive(Debug)]
            pub struct [<$name Deleted>](pub $name);

            impl MetadataModel for [<$name Deleted>] {
                type KeyType = <$name as MetadataModel>::KeyType;
                type ProstType = <$name as MetadataModel>::ProstType;

                fn cf_name() -> String {
                    $cf.to_string()
                }

                fn to_protobuf(&self) -> Self::ProstType {
                    self.0.clone()
                }

                fn from_protobuf(prost: Self::ProstType) -> Self {
                    Self($name::from_protobuf(prost))
                }

                fn key(&self) -> MetadataModelResult<Self::KeyType> {
                    self.0.key()
                }
            }
        }
    };
}

/// Column family name for deleted table catalog.
const CATALOG_TABLE_DELETED_CF_NAME: &str = "cf/deleted_catalog_table";
/// Column family name for deleted sink catalog.
const CATALOG_SINK_DELETED_CF_NAME: &str = "cf/deleted_catalog_sink";

create_catalog_deleted!(Table, CATALOG_TABLE_DELETED_CF_NAME);
create_catalog_deleted!(Sink, CATALOG_SINK_DELETED_CF_NAME);

#[derive(Debug)]
pub enum CatalogDeletedId {
    TableId(TableId),
    SinkId(TableId),
}

impl CatalogDeletedId {
    pub fn get_id(&self) -> &TableId {
        match self {
            CatalogDeletedId::SinkId(id) | CatalogDeletedId::TableId(id) => id,
        }
    }
}

pub struct TableBackgroundDeleter(mpsc::UnboundedSender<Vec<CatalogDeletedId>>);

impl TableBackgroundDeleter {
    pub async fn new<S: MetaStore>(
        env: MetaSrvEnv<S>,
        stream_manager: GlobalStreamManagerRef<S>,
    ) -> MetaResult<(Self, JoinHandle<()>, Sender<()>)> {
        let env_clone = env.clone();
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<CatalogDeletedId>>();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

        let join_handle = tokio::spawn(async move {
            loop {
                let catalog_ids = tokio::select! {
                    _ = &mut shutdown_rx => {
                        tracing::info!("Table background deleter is stopped");
                        return;
                    }
                    catalog_ids = rx.recv() => {
                        catalog_ids
                    }
                };

                if let Some(catalog_ids) = catalog_ids {
                    // TODO(zehua): add retry and batch.
                    for id in catalog_ids.iter().map(CatalogDeletedId::get_id) {
                        stream_manager.drop_materialized_view(id).await.ok();
                    }

                    for id in catalog_ids {
                        match id {
                            CatalogDeletedId::SinkId(id) => {
                                SinkDeleted::delete(env.meta_store(), &id.table_id)
                                    .await
                                    .ok();
                            }
                            CatalogDeletedId::TableId(id) => {
                                TableDeleted::delete(env.meta_store(), &id.table_id)
                                    .await
                                    .ok();
                            }
                        }
                    }
                }
            }
        });

        let background_deleter = Self(tx);

        background_deleter.init(env_clone).await?;

        Ok((background_deleter, join_handle, shutdown_tx))
    }

    async fn init<S: MetaStore>(&self, env: MetaSrvEnv<S>) -> MetaResult<()> {
        let mut catalog_ids = vec![];
        catalog_ids.extend(
            TableDeleted::list(env.meta_store())
                .await?
                .into_iter()
                .map(|table_deleted| CatalogDeletedId::TableId(table_deleted.0.id.into())),
        );
        catalog_ids.extend(
            SinkDeleted::list(env.meta_store())
                .await?
                .into_iter()
                .map(|sink_deleted| CatalogDeletedId::SinkId(sink_deleted.0.id.into())),
        );
        self.delete(catalog_ids);
        Ok(())
    }

    pub fn delete(&self, catalog_ids: Vec<CatalogDeletedId>) {
        self.0.send(catalog_ids).unwrap()
    }
}
