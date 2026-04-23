// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc, RwLock};

use futures::StreamExt;
use futures::channel::mpsc::{Sender, channel};
use tokio::sync::Notify;

use crate::runtime::spawn;
use crate::scan::{DeleteFileContext, FileScanTask};
use crate::spec::{DataContentType, DataFile, Struct};

// Iceberg field id for the `file_path` column in position delete files.
//
// See Iceberg spec (position deletes) and our `POSITION_DELETE_SCHEMA` in
// `writer/base_writer/position_delete_file_writer.rs`.
const FIELD_ID_POSITIONAL_DELETE_FILE_PATH: i32 = 2147483546;

fn try_infer_single_referenced_data_file_from_bounds(delete_file: &DataFile) -> Option<String> {
    // Match Iceberg Java's `DeleteFileUtil.referencedDataFile(DeleteFile)` heuristic:
    // if lower and upper bounds for PATH_ID are present and equal, the delete file
    // targets a single data file.
    let lower = delete_file
        .lower_bounds
        .get(&FIELD_ID_POSITIONAL_DELETE_FILE_PATH)?;
    let upper = delete_file
        .upper_bounds
        .get(&FIELD_ID_POSITIONAL_DELETE_FILE_PATH)?;

    if lower != upper {
        return None;
    }

    let bytes = lower.to_bytes().ok()?;
    let path = std::str::from_utf8(bytes.as_ref()).ok()?;
    Some(path.to_string())
}

/// Index of delete files
#[derive(Debug, Clone)]
pub(crate) struct DeleteFileIndex {
    state: Arc<RwLock<DeleteFileIndexState>>,
}

#[derive(Debug)]
enum DeleteFileIndexState {
    Populating(Arc<Notify>),
    Populated(PopulatedDeleteFileIndex),
}

type DeleteFileContextAndTask = (Arc<DeleteFileContext>, Arc<FileScanTask>);

#[derive(Debug)]
struct PopulatedDeleteFileIndex {
    #[allow(dead_code)]
    global_equality_deletes: Vec<DeleteFileContextAndTask>,
    eq_deletes_by_partition: HashMap<Struct, Vec<DeleteFileContextAndTask>>,
    pos_deletes_by_partition: HashMap<Struct, Vec<DeleteFileContextAndTask>>,
    // TODO: do we need this?
    // pos_deletes_by_path: HashMap<String, Vec<Arc<DeleteFileContext>>>,

    // TODO: Deletion Vector support
}

impl DeleteFileIndex {
    /// create a new `DeleteFileIndex` along with the sender that populates it with delete files
    pub(crate) fn new() -> (DeleteFileIndex, Sender<DeleteFileContext>) {
        // TODO: what should the channel limit be?
        let (tx, rx) = channel(10);
        let notify = Arc::new(Notify::new());
        let state = Arc::new(RwLock::new(DeleteFileIndexState::Populating(
            notify.clone(),
        )));
        let delete_file_stream = rx.boxed();

        spawn({
            let state = state.clone();
            async move {
                let delete_files: Vec<DeleteFileContext> =
                    delete_file_stream.collect::<Vec<_>>().await;

                let populated_delete_file_index = PopulatedDeleteFileIndex::new(delete_files);

                {
                    let mut guard = state.write().unwrap();
                    *guard = DeleteFileIndexState::Populated(populated_delete_file_index);
                }
                notify.notify_waiters();
            }
        });

        (DeleteFileIndex { state }, tx)
    }

    /// Gets all the delete files that apply to the specified data file.
    pub(crate) async fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<Arc<FileScanTask>> {
        let notifier = {
            let guard = self.state.read().unwrap();
            match *guard {
                DeleteFileIndexState::Populating(ref notifier) => notifier.clone(),
                DeleteFileIndexState::Populated(ref index) => {
                    return index.get_deletes_for_data_file(data_file, seq_num);
                }
            }
        };

        notifier.notified().await;

        let guard = self.state.read().unwrap();
        match guard.deref() {
            DeleteFileIndexState::Populated(index) => {
                index.get_deletes_for_data_file(data_file, seq_num)
            }
            _ => unreachable!("Cannot be any other state than loaded"),
        }
    }
}

impl PopulatedDeleteFileIndex {
    /// Creates a new populated delete file index from a list of delete file contexts, which
    /// allows for fast lookup when determining which delete files apply to a given data file.
    ///
    /// 1. The partition information is extracted from each delete file's manifest entry.
    /// 2. If the partition is empty and the delete file is not a positional delete,
    ///    it is added to the `global_equality_deletes` vector
    /// 3. Otherwise, the delete file is added to one of two hash maps based on its content type.
    fn new(files: Vec<DeleteFileContext>) -> PopulatedDeleteFileIndex {
        let mut eq_deletes_by_partition = HashMap::default();
        let mut pos_deletes_by_partition = HashMap::default();

        let mut global_equality_deletes: Vec<(Arc<DeleteFileContext>, Arc<FileScanTask>)> = vec![];

        files.into_iter().for_each(|ctx| {
            let arc_ctx = Arc::new(ctx);
            let file_scan_task: Arc<FileScanTask> = Arc::new(arc_ctx.as_ref().into());

            let partition = arc_ctx.manifest_entry.data_file().partition();

            // The spec states that "Equality delete files stored with an unpartitioned spec are applied as global deletes".
            if partition.fields().is_empty() {
                // TODO: confirm we're good to skip here if we encounter a pos del
                if arc_ctx.manifest_entry.content_type() != DataContentType::PositionDeletes {
                    global_equality_deletes.push((arc_ctx, file_scan_task.clone()));
                    return;
                }
            }

            let destination_map = match arc_ctx.manifest_entry.content_type() {
                DataContentType::PositionDeletes => &mut pos_deletes_by_partition,
                DataContentType::EqualityDeletes => &mut eq_deletes_by_partition,
                _ => unreachable!(),
            };

            destination_map
                .entry(partition.clone())
                .and_modify(|entry: &mut Vec<DeleteFileContextAndTask>| {
                    entry.push((arc_ctx.clone(), file_scan_task.clone()));
                })
                .or_insert(vec![(arc_ctx.clone(), file_scan_task)]);
        });

        PopulatedDeleteFileIndex {
            global_equality_deletes,
            eq_deletes_by_partition,
            pos_deletes_by_partition,
        }
    }

    /// Determine all the delete files that apply to the provided `DataFile`.
    fn get_deletes_for_data_file(
        &self,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<Arc<FileScanTask>> {
        let mut results = vec![];

        self.global_equality_deletes
            .iter()
            // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
            .filter(|&(delete, _)| {
                seq_num
                    .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                    .unwrap_or(true)
            })
            .for_each(|(_, task)| results.push(task.clone()));

        if let Some(deletes) = self.eq_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than** `seq_num`
                .filter(|&(delete, _)| {
                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() > Some(seq_num))
                        .unwrap_or(true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                })
                .for_each(|(_, task)| results.push(task.clone()));
        }

        // Per the spec, position deletes with a non-null `referenced_data_file` apply only to the
        // matching data file (exact match against the data file's `file_path`).
        //
        // Note: if `referenced_data_file` is null, a position delete file may contain deletes for
        // multiple data files. In that case, additional filtering must happen when reading the
        // position delete file contents (using the `file_path` column).
        //
        // Iceberg Java additionally derives an implicit referenced data file from
        // `lower_bounds`/`upper_bounds` for the position delete's `file_path` column when those
        // bounds indicate a single value. We apply the same pruning here.
        if let Some(deletes) = self.pos_deletes_by_partition.get(data_file.partition()) {
            deletes
                .iter()
                // filter that returns true if the provided delete file's sequence number is **greater than or equal to** `seq_num`
                .filter(|&(delete, _)| {
                    let delete_data_file = delete.manifest_entry.data_file();

                    let referenced_data_file_matches = match delete_data_file
                        .referenced_data_file
                        .as_deref()
                    {
                        Some(referenced_data_file) => referenced_data_file == data_file.file_path(),
                        None => match try_infer_single_referenced_data_file_from_bounds(
                            delete_data_file,
                        ) {
                            Some(referenced_data_file) => {
                                referenced_data_file == data_file.file_path()
                            }
                            None => true,
                        },
                    };

                    seq_num
                        .map(|seq_num| delete.manifest_entry.sequence_number() >= Some(seq_num))
                        .unwrap_or(true)
                        && data_file.partition_spec_id == delete.partition_spec_id
                        && referenced_data_file_matches
                })
                .for_each(|(_, task)| results.push(task.clone()));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use futures::SinkExt;
    use uuid::Uuid;

    use super::*;
    use crate::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, Literal, ManifestEntry,
        ManifestStatus, Schema, Struct,
    };

    #[test]
    fn test_delete_file_index_unpartitioned() {
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_eq_delete()),
            build_added_manifest_entry(5, &build_unpartitioned_pos_delete()),
            build_added_manifest_entry(6, &build_unpartitioned_pos_delete()),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: 0,
                snapshot_schema: Arc::new(Schema::builder().with_schema_id(1).build().unwrap()), // hack
                field_ids: Arc::new(vec![]), // hack
                case_sensitive: false,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let data_file = build_unpartitioned_data_file();

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.data_file_path.clone())
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.data_file_path.clone())
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&data_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.data_file_path.clone())
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // The 2 global equality deletes should match against any partitioned file
        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), 1);

        let delete_files_to_apply_for_partitioned_file =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        let actual_paths_to_apply_for_partitioned_file: Vec<String> =
            delete_files_to_apply_for_partitioned_file
                .into_iter()
                .map(|file| file.data_file_path.clone())
                .collect();
        assert_eq!(
            actual_paths_to_apply_for_partitioned_file,
            delete_file_paths[..2]
        );
    }

    #[test]
    fn test_delete_file_index_partitioned() {
        let partition_one = Struct::from_iter([Some(Literal::long(100))]);
        let spec_id = 1;
        let deletes: Vec<ManifestEntry> = vec![
            build_added_manifest_entry(4, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_eq_delete(&partition_one, spec_id)),
            build_added_manifest_entry(5, &build_partitioned_pos_delete(&partition_one, spec_id)),
            build_added_manifest_entry(6, &build_partitioned_pos_delete(&partition_one, spec_id)),
        ];

        let delete_file_paths: Vec<String> = deletes
            .iter()
            .map(|file| file.file_path().to_string())
            .collect();

        let delete_contexts: Vec<DeleteFileContext> = deletes
            .into_iter()
            .map(|entry| DeleteFileContext {
                manifest_entry: entry.into(),
                partition_spec_id: spec_id,
                snapshot_schema: Arc::new(Schema::builder().with_schema_id(1).build().unwrap()), // hack
                field_ids: Arc::new(vec![]), // hack
                case_sensitive: false,
            })
            .collect();

        let delete_file_index = PopulatedDeleteFileIndex::new(delete_contexts);

        let partitioned_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(100))]), spec_id);

        // All deletes apply to sequence 0
        let delete_files_to_apply_for_seq_0 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(0));
        assert_eq!(delete_files_to_apply_for_seq_0.len(), 4);

        // All deletes apply to sequence 3
        let delete_files_to_apply_for_seq_3 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(3));
        assert_eq!(delete_files_to_apply_for_seq_3.len(), 4);

        // Last 3 deletes apply to sequence 4
        let delete_files_to_apply_for_seq_4 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(4));
        let actual_paths_to_apply_for_seq_4: Vec<String> = delete_files_to_apply_for_seq_4
            .into_iter()
            .map(|file| file.data_file_path.clone())
            .collect();

        assert_eq!(
            actual_paths_to_apply_for_seq_4,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Last 3 deletes apply to sequence 5
        let delete_files_to_apply_for_seq_5 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(5));
        let actual_paths_to_apply_for_seq_5: Vec<String> = delete_files_to_apply_for_seq_5
            .into_iter()
            .map(|file| file.data_file_path.clone())
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_5,
            delete_file_paths[delete_file_paths.len() - 3..]
        );

        // Only the last position delete applies to sequence 6
        let delete_files_to_apply_for_seq_6 =
            delete_file_index.get_deletes_for_data_file(&partitioned_file, Some(6));
        let actual_paths_to_apply_for_seq_6: Vec<String> = delete_files_to_apply_for_seq_6
            .into_iter()
            .map(|file| file.data_file_path.clone())
            .collect();
        assert_eq!(
            actual_paths_to_apply_for_seq_6,
            delete_file_paths[delete_file_paths.len() - 1..]
        );

        // Data file with different partition tuples does not match any delete files
        let partitioned_second_file =
            build_partitioned_data_file(&Struct::from_iter([Some(Literal::long(200))]), 1);
        let delete_files_to_apply_for_different_partition =
            delete_file_index.get_deletes_for_data_file(&partitioned_second_file, Some(0));
        let actual_paths_to_apply_for_different_partition: Vec<String> =
            delete_files_to_apply_for_different_partition
                .into_iter()
                .map(|file| file.data_file_path.clone())
                .collect();
        assert!(actual_paths_to_apply_for_different_partition.is_empty());

        // Data file with same tuple but different spec ID does not match any delete files
        let partitioned_different_spec = build_partitioned_data_file(&partition_one, 2);
        let delete_files_to_apply_for_different_spec =
            delete_file_index.get_deletes_for_data_file(&partitioned_different_spec, Some(0));
        let actual_paths_to_apply_for_different_spec: Vec<String> =
            delete_files_to_apply_for_different_spec
                .into_iter()
                .map(|file| file.data_file_path.clone())
                .collect();
        assert!(actual_paths_to_apply_for_different_spec.is_empty());
    }

    fn build_unpartitioned_eq_delete() -> DataFile {
        build_partitioned_eq_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_eq_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}_equality_delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::EqualityDeletes)
            .equality_ids(Some(vec![1]))
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_unpartitioned_pos_delete() -> DataFile {
        build_partitioned_pos_delete(&Struct::empty(), 0)
    }

    fn build_partitioned_pos_delete(partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-pos-delete.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn test_partition() -> Struct {
        Struct::from_iter([Some(Literal::long(100))])
    }

    fn build_test_data_file(path: &str, partition: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_test_pos_delete_file(
        path: &str,
        referenced_data_file: Option<&str>,
        bounds: Option<(Datum, Datum)>,
        partition: &Struct,
        spec_id: i32,
    ) -> DataFile {
        let mut builder_binding = DataFileBuilder::default();
        let mut builder = builder_binding
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::PositionDeletes)
            .record_count(1)
            .partition(partition.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100);

        if let Some(referenced_data_file) = referenced_data_file {
            builder = builder.referenced_data_file(Some(referenced_data_file.to_string()));
        } else {
            builder = builder.referenced_data_file(None);
        }

        if let Some((lower_bound, upper_bound)) = bounds {
            builder = builder
                .lower_bounds(HashMap::from([(
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH,
                    lower_bound,
                )]))
                .upper_bounds(HashMap::from([(
                    FIELD_ID_POSITIONAL_DELETE_FILE_PATH,
                    upper_bound,
                )]));
        }

        builder.build().unwrap()
    }

    fn build_delete_context(entry: ManifestEntry, spec_id: i32) -> DeleteFileContext {
        DeleteFileContext {
            manifest_entry: entry.into(),
            partition_spec_id: spec_id,
            snapshot_schema: Arc::new(Schema::builder().with_schema_id(1).build().unwrap()),
            field_ids: Arc::new(vec![]),
            case_sensitive: false,
        }
    }

    fn build_populated_delete_file_index(
        spec_id: i32,
        deletes: Vec<(i64, DataFile)>,
    ) -> PopulatedDeleteFileIndex {
        let delete_contexts = deletes
            .into_iter()
            .map(|(seq_num, file)| {
                build_delete_context(build_added_manifest_entry(seq_num, &file), spec_id)
            })
            .collect();

        PopulatedDeleteFileIndex::new(delete_contexts)
    }

    fn get_sorted_delete_paths(
        delete_file_index: &PopulatedDeleteFileIndex,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<String> {
        let mut delete_paths: Vec<String> = delete_file_index
            .get_deletes_for_data_file(data_file, seq_num)
            .into_iter()
            .map(|task| task.data_file_path.clone())
            .collect();
        delete_paths.sort();
        delete_paths
    }

    async fn get_sorted_delete_paths_async(
        delete_file_index: &DeleteFileIndex,
        data_file: &DataFile,
        seq_num: Option<i64>,
    ) -> Vec<String> {
        let mut delete_paths: Vec<String> = delete_file_index
            .get_deletes_for_data_file(data_file, seq_num)
            .await
            .into_iter()
            .map(|task| task.data_file_path.clone())
            .collect();
        delete_paths.sort();
        delete_paths
    }

    #[test]
    fn test_pos_deletes_referenced_data_file_filters_to_single_data_file() {
        let partition = test_partition();
        let spec_id = 1;

        let data_a = build_test_data_file("s3://bucket/data-a.parquet", &partition, spec_id);
        let data_b = build_test_data_file("s3://bucket/data-b.parquet", &partition, spec_id);
        let delete_file_index = build_populated_delete_file_index(spec_id, vec![
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-a.parquet",
                    Some(data_a.file_path()),
                    None,
                    &partition,
                    spec_id,
                ),
            ),
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-b.parquet",
                    Some(data_b.file_path()),
                    None,
                    &partition,
                    spec_id,
                ),
            ),
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-any.parquet",
                    None,
                    None,
                    &partition,
                    spec_id,
                ),
            ),
        ]);

        assert_eq!(
            get_sorted_delete_paths(&delete_file_index, &data_a, Some(0)),
            vec![
                "s3://bucket/pos-delete-a.parquet".to_string(),
                "s3://bucket/pos-delete-any.parquet".to_string(),
            ]
        );

        assert_eq!(
            get_sorted_delete_paths(&delete_file_index, &data_b, Some(0)),
            vec![
                "s3://bucket/pos-delete-any.parquet".to_string(),
                "s3://bucket/pos-delete-b.parquet".to_string(),
            ]
        );
    }

    #[test]
    fn test_pos_deletes_bounds_single_file_prunes_like_referenced_data_file() {
        let partition = test_partition();
        let spec_id = 1;

        let data_a = build_test_data_file("s3://bucket/data-a.parquet", &partition, spec_id);
        let data_b = build_test_data_file("s3://bucket/data-b.parquet", &partition, spec_id);
        let delete_file_index = build_populated_delete_file_index(spec_id, vec![
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-a.parquet",
                    None,
                    Some((
                        Datum::string(data_a.file_path()),
                        Datum::string(data_a.file_path()),
                    )),
                    &partition,
                    spec_id,
                ),
            ),
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-any.parquet",
                    None,
                    None,
                    &partition,
                    spec_id,
                ),
            ),
        ]);

        assert_eq!(
            get_sorted_delete_paths(&delete_file_index, &data_a, Some(0)),
            vec![
                "s3://bucket/pos-delete-a.parquet".to_string(),
                "s3://bucket/pos-delete-any.parquet".to_string(),
            ]
        );

        assert_eq!(
            get_sorted_delete_paths(&delete_file_index, &data_b, Some(0)),
            vec!["s3://bucket/pos-delete-any.parquet".to_string(),]
        );
    }

    #[test]
    fn test_pos_deletes_bounds_range_does_not_prune() {
        let partition = test_partition();
        let spec_id = 1;

        let data_a = build_test_data_file("s3://bucket/data-a.parquet", &partition, spec_id);
        let data_b = build_test_data_file("s3://bucket/data-b.parquet", &partition, spec_id);
        let delete_file_index = build_populated_delete_file_index(spec_id, vec![(
            10,
            build_test_pos_delete_file(
                "s3://bucket/pos-delete-range.parquet",
                None,
                Some((
                    Datum::string(data_a.file_path()),
                    Datum::string(data_b.file_path()),
                )),
                &partition,
                spec_id,
            ),
        )]);

        let deletes_for_a = delete_file_index.get_deletes_for_data_file(&data_a, Some(0));
        assert_eq!(deletes_for_a.len(), 1);

        let deletes_for_b = delete_file_index.get_deletes_for_data_file(&data_b, Some(0));
        assert_eq!(deletes_for_b.len(), 1);
    }

    #[test]
    fn test_try_infer_referenced_data_file_missing_bounds_returns_none() {
        let pos_delete = build_test_pos_delete_file(
            "s3://bucket/pos-delete-missing-bounds.parquet",
            None,
            None,
            &Struct::empty(),
            0,
        );

        assert!(try_infer_single_referenced_data_file_from_bounds(&pos_delete).is_none());
    }

    #[test]
    fn test_pos_deletes_referenced_data_file_still_respects_sequence_number() {
        let partition = test_partition();
        let spec_id = 1;

        let data_a = build_test_data_file("s3://bucket/data-a.parquet", &partition, spec_id);
        let delete_file_index = build_populated_delete_file_index(spec_id, vec![
            (
                5,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-old.parquet",
                    Some(data_a.file_path()),
                    None,
                    &partition,
                    spec_id,
                ),
            ),
            (
                6,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-new.parquet",
                    Some(data_a.file_path()),
                    None,
                    &partition,
                    spec_id,
                ),
            ),
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-other.parquet",
                    Some("s3://bucket/data-b.parquet"),
                    None,
                    &partition,
                    spec_id,
                ),
            ),
        ]);

        assert_eq!(
            get_sorted_delete_paths(&delete_file_index, &data_a, Some(6)),
            vec!["s3://bucket/pos-delete-new.parquet".to_string()]
        );
    }

    #[test]
    fn test_pos_deletes_invalid_bounds_fall_back_to_no_pruning() {
        let partition = test_partition();
        let spec_id = 1;

        let data_a = build_test_data_file("s3://bucket/data-a.parquet", &partition, spec_id);
        let data_b = build_test_data_file("s3://bucket/data-b.parquet", &partition, spec_id);
        let delete_file_index = build_populated_delete_file_index(spec_id, vec![(
            10,
            build_test_pos_delete_file(
                "s3://bucket/pos-delete-invalid-bounds.parquet",
                None,
                Some((
                    Datum::binary(vec![0xff, 0xfe]),
                    Datum::binary(vec![0xff, 0xfe]),
                )),
                &partition,
                spec_id,
            ),
        )]);

        assert_eq!(
            get_sorted_delete_paths(&delete_file_index, &data_a, Some(0)).len(),
            1
        );
        assert_eq!(
            get_sorted_delete_paths(&delete_file_index, &data_b, Some(0)).len(),
            1
        );
    }

    #[tokio::test]
    async fn test_async_delete_file_index_population_applies_referenced_data_file_pruning() {
        let partition = test_partition();
        let spec_id = 1;
        let data_a = build_test_data_file("s3://bucket/data-a.parquet", &partition, spec_id);
        let data_b = build_test_data_file("s3://bucket/data-b.parquet", &partition, spec_id);

        let (delete_file_index, mut delete_file_tx) = DeleteFileIndex::new();
        for (seq_num, file) in [
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-a.parquet",
                    Some(data_a.file_path()),
                    None,
                    &partition,
                    spec_id,
                ),
            ),
            (
                10,
                build_test_pos_delete_file(
                    "s3://bucket/pos-delete-any.parquet",
                    None,
                    None,
                    &partition,
                    spec_id,
                ),
            ),
        ] {
            delete_file_tx
                .send(build_delete_context(
                    build_added_manifest_entry(seq_num, &file),
                    spec_id,
                ))
                .await
                .unwrap();
        }
        drop(delete_file_tx);

        assert_eq!(
            get_sorted_delete_paths_async(&delete_file_index, &data_a, Some(0)).await,
            vec![
                "s3://bucket/pos-delete-a.parquet".to_string(),
                "s3://bucket/pos-delete-any.parquet".to_string(),
            ]
        );

        assert_eq!(
            get_sorted_delete_paths_async(&delete_file_index, &data_b, Some(0)).await,
            vec!["s3://bucket/pos-delete-any.parquet".to_string()]
        );
    }

    fn build_unpartitioned_data_file() -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(Struct::empty())
            .partition_spec_id(0)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_partitioned_data_file(partition_value: &Struct, spec_id: i32) -> DataFile {
        DataFileBuilder::default()
            .file_path(format!("{}-data.parquet", Uuid::new_v4()))
            .file_format(DataFileFormat::Parquet)
            .content(DataContentType::Data)
            .record_count(100)
            .partition(partition_value.clone())
            .partition_spec_id(spec_id)
            .file_size_in_bytes(100)
            .build()
            .unwrap()
    }

    fn build_added_manifest_entry(data_seq_number: i64, file: &DataFile) -> ManifestEntry {
        ManifestEntry::builder()
            .status(ManifestStatus::Added)
            .sequence_number(data_seq_number)
            .data_file(file.clone())
            .build()
    }
}
