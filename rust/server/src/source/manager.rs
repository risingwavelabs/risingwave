use crate::source::{FileSource, KafkaSource, Source, SourceConfig, SourceFormat};
use risingwave_common::catalog::TableId;
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataTypeRef;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub type SourceRef = Arc<dyn Source>;

pub trait SourceManager: Sync + Send {
    fn create_source(
        &self,
        source_id: &TableId,
        format: SourceFormat,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
    ) -> Result<()>;
    fn get_source(&self, source_id: &TableId) -> Result<SourceDesc>;
    fn drop_source(&self, source_id: &TableId) -> Result<()>;
}

#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub data_type: DataTypeRef,
    pub column_id: i32,
}

#[derive(Clone)]
pub struct SourceDesc {
    pub source: SourceRef,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
}

pub type SourceManagerRef = Arc<dyn SourceManager>;

pub struct MemSourceManager {
    sources: Mutex<HashMap<TableId, SourceDesc>>,
}

impl SourceManager for MemSourceManager {
    fn create_source(
        &self,
        table_id: &TableId,
        format: SourceFormat,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
    ) -> Result<()> {
        let mut tables = self.get_tables()?;

        ensure!(
            !tables.contains_key(table_id),
            "Source id already exists: {:?}",
            table_id
        );

        let source: Arc<dyn Source> = match config {
            SourceConfig::Kafka(_) => Arc::new(KafkaSource::new(config.clone())?),
            SourceConfig::File(_) => Arc::new(FileSource::new(config.clone())?),
        };

        let desc = SourceDesc {
            source,
            format,
            columns,
        };

        tables.insert(table_id.clone(), desc);

        Ok(())
    }

    fn get_source(&self, table_id: &TableId) -> Result<SourceDesc> {
        let tables = self.get_tables()?;
        tables
            .get(table_id)
            .cloned()
            .ok_or_else(|| InternalError(format!("Table id not exists: {:?}", table_id)).into())
    }

    fn drop_source(&self, table_id: &TableId) -> Result<()> {
        let mut tables = self.get_tables()?;
        ensure!(
            tables.contains_key(table_id),
            "Table does not exist: {:?}",
            table_id
        );
        tables.remove(table_id);
        Ok(())
    }
}

impl MemSourceManager {
    pub fn new() -> Self {
        MemSourceManager {
            sources: Mutex::new(HashMap::new()),
        }
    }

    fn get_tables(&self) -> Result<MutexGuard<HashMap<TableId, SourceDesc>>> {
        self.sources.lock().map_err(|e| {
            RwError::from(ErrorCode::InternalError(format!(
                "failed to acquire storage manager lock: {}",
                e
            )))
        })
    }
}
