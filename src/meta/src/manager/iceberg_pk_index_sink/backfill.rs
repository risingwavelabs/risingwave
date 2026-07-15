// Copyright 2026 RisingWave Labs
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

use std::collections::HashMap;

use anyhow::Result;
use iceberg::spec::DataFile;

pub fn backfill_delete_file_partitions(
    writer_files: &[DataFile],
    delete_files: &mut [DataFile],
) -> Result<()> {
    if delete_files.is_empty() {
        return Ok(());
    }
    let mut waiting_delete_files = delete_files
        .iter_mut()
        .filter(|dv| dv.partition().fields().is_empty())
        .map(|dv| (dv.referenced_data_file().unwrap(), dv))
        .collect::<HashMap<_, _>>();
    if waiting_delete_files.is_empty() {
        return Ok(());
    }

    for f in writer_files {
        if let Some(dv) = waiting_delete_files.remove(f.file_path()) {
            dv.set_partition(f.partition().clone());
            if waiting_delete_files.is_empty() {
                return Ok(());
            }
        }
    }

    if !waiting_delete_files.is_empty() {
        anyhow::bail!(
            "backfill iceberg pk-index sink delete files failed, missing referenced data files: {:?}",
            waiting_delete_files.keys().collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Literal, Struct};

    use super::*;

    fn writer_file(path: &str, partition: Struct) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .build()
            .expect("writer file build")
    }

    fn dv_file(path: &str, referenced: &str, partition: Struct) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::PositionDeletes)
            .file_path(path.to_owned())
            .file_format(DataFileFormat::Puffin)
            .partition(partition)
            .partition_spec_id(0)
            .record_count(1)
            .file_size_in_bytes(1)
            .referenced_data_file(Some(referenced.to_owned()))
            .build()
            .expect("dv file build")
    }

    fn struct_with_int(value: i32) -> Struct {
        Struct::from_iter([Some(Literal::int(value))])
    }

    #[test]
    fn empty_inputs_noop() -> Result<()> {
        let mut dvs: Vec<DataFile> = vec![];
        backfill_delete_file_partitions(&[], &mut dvs)?;
        assert!(dvs.is_empty());

        let writers = [writer_file("data/a.parquet", struct_with_int(7))];
        let mut dvs: Vec<DataFile> = vec![];
        backfill_delete_file_partitions(&writers, &mut dvs)?;

        assert!(dvs.is_empty());
        Ok(())
    }

    #[test]
    fn dv_referencing_same_epoch_writer_gets_partition() -> Result<()> {
        let writers = [writer_file("data/a.parquet", struct_with_int(42))];
        let mut dvs = vec![dv_file("dv/a.puff", "data/a.parquet", Struct::empty())];

        backfill_delete_file_partitions(&writers, &mut dvs)?;

        assert_eq!(dvs[0].partition(), &struct_with_int(42));
        Ok(())
    }

    #[test]
    fn dv_referencing_older_snapshot_left_alone() -> Result<()> {
        // DV references a data file NOT in the uncommitted writer set — its
        // partition was correctly populated by PositionDeleteMerger from the older
        // snapshot and must not be overwritten.
        let writers = [writer_file("data/a.parquet", struct_with_int(1))];
        let dv_partition = struct_with_int(99);
        let mut dvs = vec![dv_file(
            "dv/old.puff",
            "data/somewhere_old.parquet",
            dv_partition.clone(),
        )];

        backfill_delete_file_partitions(&writers, &mut dvs)?;

        assert_eq!(dvs[0].partition(), &dv_partition, "must not overwrite");
        Ok(())
    }

    #[test]
    fn mixed_set_partial_backfill() -> Result<()> {
        let writers = [
            writer_file("data/a.parquet", struct_with_int(1)),
            writer_file("data/b.parquet", struct_with_int(2)),
        ];
        let mut dvs = vec![
            dv_file("dv/a.puff", "data/a.parquet", Struct::empty()),
            dv_file("dv/old.puff", "data/c.parquet", struct_with_int(99)),
            dv_file("dv/b.puff", "data/b.parquet", Struct::empty()),
        ];

        backfill_delete_file_partitions(&writers, &mut dvs)?;

        assert_eq!(dvs[0].partition(), &struct_with_int(1));
        assert_eq!(
            dvs[1].partition(),
            &struct_with_int(99),
            "older snapshot DV untouched"
        );
        assert_eq!(dvs[2].partition(), &struct_with_int(2));
        Ok(())
    }
}
