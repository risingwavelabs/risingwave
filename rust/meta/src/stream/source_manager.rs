use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::error::{Result, ToRwResult};
use risingwave_connector::base::SplitEnumerator;
use risingwave_connector::{extract_split_enumerator, SplitEnumeratorImpl};
use risingwave_pb::plan::TableRefId;
use tokio::sync::Mutex;

use crate::barrier::BarrierManagerRef;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

pub struct SourceManager<S>
where
    S: MetaStore,
{
    meta_store_ref: Arc<S>,
    barrier_manager_ref: BarrierManagerRef<S>,
    enumerators: Arc<Mutex<HashMap<u32, SplitEnumeratorImpl>>>,
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
            enumerators: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn run(&self) -> Result<()> {
        todo!()
    }

    pub async fn create_source(&self, ctx: CreateSourceContext) -> Result<()> {
        let mut enumerator = extract_split_enumerator(&ctx.properties).to_rw_result()?;
        let splits = enumerator.list_splits().await.to_rw_result()?;
        let _ = splits;
        todo!()
    }

    pub async fn drop_source(&self, _ctx: DropSourceContext) -> Result<()> {
        todo!()
    }

    pub async fn list_splits(&self, _table_id: TableRefId) -> Result<()> {
        todo!()
    }

    async fn assign_splits(&self) -> Result<()> {
        todo!()
        // self.barrier_manager_ref.run_command()
    }
}
