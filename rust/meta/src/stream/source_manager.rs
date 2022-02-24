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

use crate::manager::StoredCatalogManagerRef;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub struct SourceManager<S>
    where
        S: MetaStore,
{
    catalog_manager_ref: StoredCatalogManagerRef<S>,
    enumerators: DashMap<TableRefId, Arc<SplitEnumeratorImpl>>,
}

pub struct CreateSourceContext {
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
        Ok(Self {
            catalog_manager_ref,
            enumerators: DashMap::new(),
        })
    }

    pub async fn create_source(&self, ctx: CreateSourceContext) -> Result<()> {
        match self.enumerators.entry(ctx.table_id) {
            Entry::Occupied(_) => {
                return Err(format!("source already exists: {:?}", ctx.table_id).into());
            }
            Entry::Vacant(e) => {
                let enumerator = extract_split_enumerator(&ctx.properties)?;
                e.insert(Arc::new(enumerator));
            }
        }

        todo!()
    }

    pub async fn drop_source(&self, _ctx: DropSourceContext) -> Result<()> {
        todo!()
    }

    pub async fn list_splits(&self, table_id: TableRefId) -> Result<()> {
        // let x = self.enumerators.entry(&table_id).map(|e| {
        //
        //     enumerator.list_splits().await
        // });
        //

        match self.enumerators.entry(table_id) {
            Entry::Occupied(e) => {

            }
            Entry::Vacant(_) => {
                return Err(format!("source not found: {:?}", table_id).into());
            }
        }


        todo!()
    }
}
