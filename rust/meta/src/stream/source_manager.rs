use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::error::Result;

use crate::barrier::BarrierManagerRef;
use crate::storage::MetaStore;

pub type SourceManagerRef<S> = Arc<SourceManager<S>>;

#[allow(dead_code)]
pub struct SourceManager<S>
where
    S: MetaStore,
{
    meta_store_ref: Arc<S>,
    barrier_manager_ref: BarrierManagerRef<S>,
}

#[allow(dead_code)]
pub struct CreateSourceContext {
    pub name: String,
    pub source_id: u32,
    pub discovery_new_split: bool,
    pub properties: HashMap<String, String>,
}

#[allow(dead_code)]
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
        Ok(Self {
            meta_store_ref,
            barrier_manager_ref,
        })
    }

    pub async fn run(&self) -> Result<()> {
        // todo: fill me
        Ok(())
    }
}
