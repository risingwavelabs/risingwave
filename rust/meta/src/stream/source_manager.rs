use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use dashmap::mapref::one::Ref;
use tokio::sync::{Mutex, RwLock};

use risingwave_common::error::Result;
use risingwave_pb::plan::TableRefId;

use risingwave_connector::base::SplitEnumerator;
use risingwave_connector::{extract_split_enumerator, SplitEnumeratorImpl};
use risingwave_pb::catalog::StreamSourceInfo;

use crate::manager::StoredCatalogManagerRef;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub struct SourceManager<S>
    where
        S: MetaStore,
{
    _phantom: std::marker::PhantomData<S>,
}

pub struct CreateSourceContext {
    pub name: String,
    pub table_id: TableRefId,
    pub discovery_new_split: bool,
    pub properties: HashMap<String, String>,
}


pub struct DropSourceContext {
    pub(crate) table_id: TableRefId,
}

impl<S> SourceManager<S>
    where
        S: MetaStore,
{
    pub async fn new(catalog_manager_ref: StoredCatalogManagerRef<S>) -> Result<Self> {
        todo!()
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
