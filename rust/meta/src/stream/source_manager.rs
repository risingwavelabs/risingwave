use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use risingwave_common::error::Result;
use risingwave_connector::base::SplitEnumerator;
use risingwave_connector::{extract_split_enumerator, SplitEnumeratorImpl};
use risingwave_pb::catalog::{Source, StreamSourceInfo};
use risingwave_pb::plan::TableRefId;
use tokio::sync::{Mutex, RwLock};

use crate::barrier::BarrierManagerRef;
use crate::cluster::StoredClusterManagerRef;
use crate::manager::{MetaSrvEnv, StoredCatalogManagerRef};
use crate::model::MetadataModel;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub struct SourceManager<S>
where
    S: MetaStore,
{
    meta_store_ref: Arc<S>,
    barrier_manager_ref: BarrierManagerRef<S>,
}

pub struct CreateSourceContext {
    pub name: String,
    pub source_id: u32,
    pub discovery_new_split: bool,
    pub properties: HashMap<String, String>,
}

pub struct DropSourceContext {
    pub id: u32,
}

impl<S> SourceManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        meta_store_ref: Arc<S>,
        barrier_manager_ref: BarrierManagerRef<S>,
    ) -> Result<Self> {
        //        let sources = Source::list(&*meta_store_ref).await?;

        Ok(Self {
            meta_store_ref,
            barrier_manager_ref,
        })
    }

    pub async fn create_source(&self, ctx: CreateSourceContext) -> Result<()> {
        todo!()
    }

    pub async fn drop_source(&self, _ctx: DropSourceContext) -> Result<()> {
        todo!()
    }

    pub async fn list_splits(&self, table_id: TableRefId) -> Result<()> {
        todo!()
    }
}
