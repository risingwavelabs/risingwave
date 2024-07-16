// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future;
use std::hash::{DefaultHasher, Hash, Hasher};

use anyhow::anyhow;
use futures_async_stream::try_stream;
use futures_util::stream::StreamExt;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_common::catalog::Schema;
use risingwave_connector::sink::iceberg::IcebergConfig;

use crate::error::BatchError;
use crate::executor::{DataChunk, Executor};

/// Create a iceberg scan executor.
///
/// # Examples
///
/// ```
/// use futures_async_stream::for_await;
/// use risingwave_batch::executor::{Executor, FileSelector, IcebergScanExecutor};
/// use risingwave_common::catalog::{Field, Schema};
/// use risingwave_common::types::DataType;
/// use risingwave_connector::sink::iceberg::IcebergConfig;
///
/// #[tokio::test]
/// async fn test_iceberg_scan() {
///     let iceberg_scan_executor = IcebergScanExecutor::new(
///         IcebergConfig {
///             database_name: Some("demo_db".into()),
///             table_name: "demo_table".into(),
///             catalog_type: Some("storage".into()),
///             path: "s3://hummock001/".into(),
///             endpoint: Some("http://127.0.0.1:9301".into()),
///             access_key: "hummockadmin".into(),
///             secret_key: "hummockadmin".into(),
///             region: Some("us-east-1".into()),
///             ..Default::default()
///         },
///         None,
///         FileSelector::select_all(),
///         1024,
///         Schema::new(vec![
///             Field::with_name(DataType::Int64, "seq_id"),
///             Field::with_name(DataType::Int64, "user_id"),
///             Field::with_name(DataType::Varchar, "user_name"),
///         ]),
///         "iceberg_scan".into(),
///     );
///
///     let stream = Box::new(iceberg_scan_executor).execute();
///     #[for_await]
///     for chunk in stream {
///         let chunk = chunk.unwrap();
///         println!("{:?}", chunk);
///     }
/// }
/// ```

pub struct IcebergScanExecutor {
    iceberg_config: IcebergConfig,
    snapshot_id: Option<i64>,
    file_selector: FileSelector,
    batch_size: usize,

    schema: Schema,
    identity: String,
}

impl Executor for IcebergScanExecutor {
    fn schema(&self) -> &risingwave_common::catalog::Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> super::BoxedDataChunkStream {
        self.do_execute().boxed()
    }
}

impl IcebergScanExecutor {
    pub fn new(
        iceberg_config: IcebergConfig,
        snapshot_id: Option<i64>,
        file_selector: FileSelector,
        batch_size: usize,
        schema: Schema,
        identity: String,
    ) -> Self {
        Self {
            iceberg_config,
            snapshot_id,
            file_selector,
            batch_size,
            schema,
            identity,
        }
    }

    #[try_stream(ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let table = self.iceberg_config.load_table_v2().await?;

        let snapshot_id = if let Some(snapshot_id) = self.snapshot_id {
            snapshot_id
        } else {
            table
                .metadata()
                .current_snapshot()
                .ok_or_else(|| {
                    BatchError::Internal(anyhow!("No snapshot found for iceberg table"))
                })?
                .snapshot_id()
        };
        let scan = table
            .scan()
            .snapshot_id(snapshot_id)
            .with_batch_size(Some(self.batch_size))
            .select(self.schema.names())
            .build()
            .map_err(|e| BatchError::Internal(anyhow!(e)))?;

        let file_selector = self.file_selector.clone();
        let file_scan_stream = scan
            .plan_files()
            .await
            .map_err(BatchError::Iceberg)?
            .filter(move |task| {
                let res = task
                    .as_ref()
                    .map(|task| file_selector.select(task.data_file_path()))
                    .unwrap_or(true);
                future::ready(res)
            });

        let reader = table
            .reader_builder()
            .with_batch_size(self.batch_size)
            .build();

        let record_batch_stream = reader
            .read(Box::pin(file_scan_stream))
            .map_err(BatchError::Iceberg)?;

        #[for_await]
        for record_batch in record_batch_stream {
            let record_batch = record_batch.map_err(BatchError::Iceberg)?;
            let chunk = IcebergArrowConvert.chunk_from_record_batch(&record_batch)?;
            debug_assert_eq!(chunk.data_types(), self.schema.data_types());
            yield chunk;
        }
    }
}

#[derive(Clone)]
pub enum FileSelector {
    // File paths to be scanned by this executor are specified.
    FileList(Vec<String>),
    // Data files to be scanned by this executor could be calculated by Hash(file_path) % num_tasks == task_id.
    // task_id, num_tasks
    Hash(usize, usize),
}

impl FileSelector {
    pub fn select_all() -> Self {
        FileSelector::Hash(0, 1)
    }

    pub fn select(&self, path: &str) -> bool {
        match self {
            FileSelector::FileList(paths) => paths.contains(&path.to_string()),
            FileSelector::Hash(task_id, num_tasks) => {
                let hash = Self::hash_str_to_usize(path);
                hash % num_tasks == *task_id
            }
        }
    }

    pub fn hash_str_to_usize(s: &str) -> usize {
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish() as usize
    }
}
