use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use risingwave_common::catalog::TableId;
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataTypeRef;

use crate::source::{
    HighLevelKafkaSource, Source, SourceConfig, SourceFormat, SourceParser, TableSource,
};
use crate::storage::BummockTable;

pub type SourceRef = Arc<Source>;

pub trait SourceManager: Sync + Send {
    fn create_source(
        &self,
        source_id: &TableId,
        format: SourceFormat,
        parser: Arc<dyn SourceParser>,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
    ) -> Result<()>;
    fn create_table_source(&self, table_id: &TableId, table: Arc<BummockTable>) -> Result<()>;
    fn get_source(&self, source_id: &TableId) -> Result<SourceDesc>;
    fn drop_source(&self, source_id: &TableId) -> Result<()>;
}

/// `SourceColumnDesc` is used to describe a column in the Source and is used as the column
/// counterpart in `StreamScan`
#[derive(Clone, Debug)]
pub struct SourceColumnDesc {
    pub name: String,
    pub data_type: DataTypeRef,
    pub column_id: i32,
}

/// `SourceDesc` is used to describe a `Source`
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
        parser: Arc<dyn SourceParser>,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
    ) -> Result<()> {
        let mut tables = self.get_tables()?;

        ensure!(
            !tables.contains_key(table_id),
            "Source id already exists: {:?}",
            table_id
        );

        let source = match config {
            SourceConfig::Kafka(config) => Source::HighLevelKafka(HighLevelKafkaSource::new(
                config.clone(),
                Arc::new(columns.clone()),
                parser.clone(),
            )),
        };

        let desc = SourceDesc {
            source: Arc::new(source),
            format,
            columns,
        };

        tables.insert(table_id.clone(), desc);

        Ok(())
    }

    fn create_table_source(&self, table_id: &TableId, table: Arc<BummockTable>) -> Result<()> {
        let mut tables = self.get_tables()?;

        ensure!(
            !tables.contains_key(table_id),
            "Source id already exists: {:?}",
            table_id
        );

        let columns = table
            .columns()
            .iter()
            .map(|c| SourceColumnDesc {
                name: "".to_string(),
                data_type: c.data_type.clone(),
                column_id: c.column_id,
            })
            .collect();

        let source = Source::Table(TableSource::new(table));

        // Table sources do not need columns and format
        let desc = SourceDesc {
            source: Arc::new(source),
            columns,
            format: SourceFormat::Invalid,
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
