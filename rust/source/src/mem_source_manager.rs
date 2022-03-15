use std::collections::HashMap;

use risingwave_common::catalog::{ColumnDesc, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::Result;
use risingwave_common::{ensure, gen_error};
use risingwave_connector::kinesis::source::reader::KinesisSplitReader;
use risingwave_connector::ConnectorConfig;

use crate::connector_source::ConnectorSource;
use crate::table_v2::TableSourceV2;
use crate::{SourceColumnDesc, SourceDesc, SourceFormat, SourceImpl, SourceManager, SourceParser};

#[derive(Debug)]
pub struct MemSourceManager {
    sources: HashMap<TableId, SourceDesc>,
}

impl SourceManager for MemSourceManager {
    fn create_table_source_v2(
        &mut self,
        table_id: &TableId,
        columns: Vec<ColumnDesc>,
    ) -> risingwave_common::error::Result<()> {
        ensure!(
            !self.sources.contains_key(table_id),
            "Source id already exists: {:?}",
            table_id
        );

        let source_columns = columns.iter().map(SourceColumnDesc::from).collect();
        let source = SourceImpl::TableV2(TableSourceV2::new(columns));

        // Table sources do not need columns and format
        let desc = SourceDesc {
            source: Box::new(source),
            columns: source_columns,
            format: SourceFormat::Invalid,
            row_id_index: Some(0), // always use the first column as row_id
        };

        self.sources.insert(*table_id, desc);
        Ok(())
    }

    fn create_source(
        &mut self,
        source_id: &TableId,
        format: SourceFormat,
        parser: Box<dyn SourceParser>,
        config: &ConnectorConfig,
        columns: Vec<SourceColumnDesc>,
        row_id_index: Option<usize>,
    ) -> Result<()> {
        ensure!(
            !self.sources.contains_key(source_id),
            "Source id already exists: {:?}",
            source_id
        );

        let reader = match config {
            // TODO(tabVersion): build split reader here
            ConnectorConfig::Kinesis(config) => {
                let mut runtime =
                    tokio::runtime::Runtime::new().expect("Unable to create a runtime");
                let split_reader = runtime.block_on(KinesisSplitReader::new(config.clone()));
                Box::new(split_reader)
            }
        };

        let source = ConnectorSource {
            parser,
            reader,
            column_descs: columns,
        };

        let desc = SourceDesc {
            source: Box::new(SourceImpl::Connector(source)),
            format,
            columns,
            row_id_index,
        };

        self.sources.insert(*source_id, desc);
        Ok(())
    }

    fn get_source(&mut self, table_id: &TableId) -> Result<SourceDesc> {
        self.sources.get(table_id).cloned().ok_or_else(|| {
            InternalError(format!("Get source table id not exists: {:?}", table_id)).into()
        })
    }

    fn drop_source(&mut self, source_id: &TableId) -> Result<()> {
        ensure!(
            self.sources.contains_key(source_id),
            "Source does not exist: {:?}",
            source_id
        );
        self.sources.remove(source_id);
        Ok(())
    }
}

impl Default for MemSourceManager {
    fn default() -> Self {
        Self::new()
    }
}
