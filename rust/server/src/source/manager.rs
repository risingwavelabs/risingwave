use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use risingwave_common::catalog::TableId;
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::types::DataTypeRef;
use risingwave_storage::bummock::BummockTable;

use crate::source::{
    HighLevelKafkaSource, SourceConfig, SourceFormat, SourceImpl, SourceParser, TableSource,
};

pub type SourceRef = Arc<SourceImpl>;

pub trait SourceManager: Sync + Send {
    fn create_source(
        &self,
        source_id: &TableId,
        format: SourceFormat,
        parser: Arc<dyn SourceParser>,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
        row_id_index: usize,
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
    pub skip_parse: bool,
}

/// `SourceDesc` is used to describe a `Source`
#[derive(Clone)]
pub struct SourceDesc {
    pub source: SourceRef,
    pub format: SourceFormat,
    pub columns: Vec<SourceColumnDesc>,
    pub row_id_column_id: i32,
}

pub type SourceManagerRef = Arc<dyn SourceManager>;

pub struct MemSourceManager {
    sources: Mutex<HashMap<TableId, SourceDesc>>,
}

impl SourceManager for MemSourceManager {
    fn create_source(
        &self,
        source_id: &TableId,
        format: SourceFormat,
        parser: Arc<dyn SourceParser>,
        config: &SourceConfig,
        columns: Vec<SourceColumnDesc>,
        row_id_index: usize,
    ) -> Result<()> {
        let mut tables = self.get_sources()?;

        ensure!(
            !tables.contains_key(source_id),
            "Source id already exists: {:?}",
            source_id
        );

        let row_id_column_id = columns[row_id_index].column_id;

        let source = match config {
            SourceConfig::Kafka(config) => SourceImpl::HighLevelKafka(HighLevelKafkaSource::new(
                config.clone(),
                Arc::new(columns.clone()),
                parser.clone(),
            )),
        };

        let desc = SourceDesc {
            source: Arc::new(source),
            format,
            columns,
            row_id_column_id,
        };

        tables.insert(source_id.clone(), desc);

        Ok(())
    }

    fn create_table_source(&self, table_id: &TableId, table: Arc<BummockTable>) -> Result<()> {
        let mut sources = self.get_sources()?;

        ensure!(
            !sources.contains_key(table_id),
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
                skip_parse: false,
            })
            .collect();

        let source = SourceImpl::Table(TableSource::new(table));

        // Table sources do not need columns and format
        let desc = SourceDesc {
            source: Arc::new(source),
            columns,
            format: SourceFormat::Invalid,
            row_id_column_id: 0,
        };

        sources.insert(table_id.clone(), desc);
        Ok(())
    }

    fn get_source(&self, table_id: &TableId) -> Result<SourceDesc> {
        let sources = self.get_sources()?;
        sources
            .get(table_id)
            .cloned()
            .ok_or_else(|| InternalError(format!("Table id not exists: {:?}", table_id)).into())
    }

    fn drop_source(&self, table_id: &TableId) -> Result<()> {
        let mut sources = self.get_sources()?;
        ensure!(
            sources.contains_key(table_id),
            "Source does not exist: {:?}",
            table_id
        );
        sources.remove(table_id);
        Ok(())
    }
}

impl MemSourceManager {
    pub fn new() -> Self {
        MemSourceManager {
            sources: Mutex::new(HashMap::new()),
        }
    }

    fn get_sources(&self) -> Result<MutexGuard<HashMap<TableId, SourceDesc>>> {
        self.sources.lock().map_err(|e| {
            RwError::from(ErrorCode::InternalError(format!(
                "failed to acquire storage manager lock: {}",
                e
            )))
        })
    }
}

#[cfg(test)]
mod test {
    use crate::source::*;
    use risingwave_storage::bummock::BummockTable;
    use risingwave_storage::{Table, TableColumnDesc};

    use risingwave_common::array::*;
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::error::Result;
    use risingwave_common::types::{DecimalType, Int64Type};

    use std::sync::Arc;

    const KAFKA_TOPIC_KEY: &str = "kafka.topic";
    const KAFKA_BOOTSTRAP_SERVERS_KEY: &str = "kafka.bootstrap.servers";

    #[tokio::test]
    async fn test_source() -> Result<()> {
        // init
        let table_id = TableId::default();
        let format = SourceFormat::Json;
        let parser = Arc::new(JSONParser {});

        let config = SourceConfig::Kafka(HighLevelKafkaSourceConfig {
            bootstrap_servers: KAFKA_BOOTSTRAP_SERVERS_KEY
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            topic: KAFKA_TOPIC_KEY.to_string(),
            properties: Default::default(),
        });

        let table = Arc::new(BummockTable::new(
            &TableId::default(),
            vec![TableColumnDesc {
                data_type: Arc::new(Int64Type::new(false)),
                column_id: 0,
            }],
        ));

        let chunk0 = StreamChunk::new(
            vec![Op::Insert],
            vec![column_nonnull!(I64Array, Int64Type, [0])],
            None,
        );
        table.write(&chunk0).unwrap();

        let source_columns = table
            .columns()
            .iter()
            .map(|c| SourceColumnDesc {
                name: "123".to_string(),
                data_type: c.data_type.clone(),
                column_id: c.column_id,
            })
            .collect();

        // create source
        let mem_source_manager = MemSourceManager::new();
        let new_source =
            mem_source_manager.create_source(&table_id, format, parser, &config, source_columns);
        assert!(new_source.is_ok());

        // get source
        let get_source_res = mem_source_manager.get_source(&table_id)?;
        assert_eq!(get_source_res.columns[0].name, "123");

        // drop source
        let drop_source_res = mem_source_manager.drop_source(&table_id);
        assert!(drop_source_res.is_ok());

        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let table_id = TableId::default();

        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Arc::new(DecimalType::new(false, 10, 5)?),
                },
                Field {
                    data_type: Arc::new(DecimalType::new(false, 10, 5)?),
                },
            ],
        };

        let table_columns = schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, f)| TableColumnDesc {
                data_type: f.data_type.clone(),
                column_id: i as i32, // use column index as column id
            })
            .collect();

        let bummock_table = Arc::new(BummockTable::new(&table_id, table_columns));
        let mem_source_manager = MemSourceManager::new();
        let res = mem_source_manager.create_table_source(&table_id, bummock_table);
        assert!(res.is_ok());

        // get source
        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_ok());

        // drop source
        let drop_source_res = mem_source_manager.drop_source(&table_id);
        assert!(drop_source_res.is_ok());
        let get_source_res = mem_source_manager.get_source(&table_id);
        assert!(get_source_res.is_err());

        Ok(())
    }
}
