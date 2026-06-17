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

//! Manifest-walking helpers for V3 compaction overwrite delete-set derivation.
//!
//! Compaction conflicts are resolved inside a transient stream pipeline: the writer
//! repairs its pk-index and the DvMerger reports fresh output DVs through the normal per-epoch path.
//! Meta therefore needs only the *delete* half of the coordinated overwrite — the input data files
//! and every Puffin DV that references them — which is fully recoverable from the snapshot manifests
//! by the input file paths. That is what `collect_compaction_delete_files` provides.
//!
//! Invariant: V3 input *data* files are stable across `(R, N]` (writers only APPEND data and ADD DV
//! files, never rewrite data files). The referencing DVs, however, MUST be read at `N` so no
//! `(R, N]` DV dangles after the input data files are removed by the overwrite.

use std::collections::HashSet;

use anyhow::{Context, Result};
use iceberg::spec::{DataContentType, DataFile, DataFileFormat, ManifestContentType, ManifestList};
use iceberg::table::Table;

/// Derive the OVERWRITE-delete set for a stream-resolved V3 compaction, BY PATH, reading only
/// manifest metadata (never the data/DV file bodies). Returns the input data files (as seen at the
/// table's current snapshot `N`) together with EVERY Puffin DV (at `N`) that references one of them.
///
/// The stream pipeline has already done the row-level resolution: the writer repaired its pk-index
/// and the DvMerger reported the fresh output DVs through the normal per-epoch path. Meta needs only
/// the *delete* half of the overwrite — the inputs being compacted away — which is fully recoverable
/// from the snapshot manifests by the input file paths. Walking `N`'s data manifests (rather than
/// `R`'s) is sound because V3 input data files are stable across `(R, N]`; the referencing DVs,
/// however, MUST be read at `N` so no `(R, N]` DV dangles after the inputs are removed.
pub(super) async fn collect_compaction_delete_files(
    table: &Table,
    input_file_paths: &[String],
) -> Result<Vec<DataFile>> {
    let snapshot_n = table
        .metadata()
        .current_snapshot()
        .context("iceberg v3 compaction overwrite: table has no current snapshot N")?
        .clone();
    let manifest_list_n = table
        .object_cache()
        .get_manifest_list(&snapshot_n, &table.metadata_ref())
        .await
        .context("load manifest list at N for compaction overwrite delete-set")?;

    let input_paths: HashSet<&str> = input_file_paths.iter().map(String::as_str).collect();
    let input_data_files = collect_input_data_files(table, &manifest_list_n, &input_paths).await?;
    let input_dv_files =
        collect_referencing_dv_files(table, &manifest_list_n, &input_paths).await?;

    // De-duplicate by path: a DV could theoretically be listed in more than one manifest, and an
    // overwrite must not delete the same path twice.
    let mut seen: HashSet<String> = HashSet::new();
    let mut delete_files = Vec::with_capacity(input_data_files.len() + input_dv_files.len());
    for file in input_data_files.into_iter().chain(input_dv_files) {
        if seen.insert(file.file_path().to_owned()) {
            delete_files.push(file);
        }
    }
    Ok(delete_files)
}

/// Walk snapshot `R`'s manifest list and collect the `DataFile`s whose path is an input path and
/// whose content is `Data`. These input data files are stable across `(R, N]`.
async fn collect_input_data_files(
    table: &Table,
    manifest_list: &ManifestList,
    input_paths: &HashSet<&str>,
) -> Result<Vec<DataFile>> {
    let mut out = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .context("load data manifest at R")?;
        for entry in manifest.entries() {
            if !entry.is_alive() || entry.content_type() != DataContentType::Data {
                continue;
            }
            let data_file = entry.data_file();
            if input_paths.contains(data_file.file_path()) {
                out.push(data_file.clone());
            }
        }
    }
    Ok(out)
}

/// Collect every alive Puffin DV `DataFile` (as seen at the given manifest list) that references one
/// of the input data files.
async fn collect_referencing_dv_files(
    table: &Table,
    manifest_list: &ManifestList,
    input_paths: &HashSet<&str>,
) -> Result<Vec<DataFile>> {
    let mut out = Vec::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(table.file_io())
            .await
            .context("load delete manifest")?;
        for entry in manifest.entries() {
            if !entry.is_alive()
                || entry.content_type() != DataContentType::PositionDeletes
                || entry.file_format() != DataFileFormat::Puffin
            {
                continue;
            }
            let data_file = entry.data_file();
            let Some(referenced) = data_file.referenced_data_file() else {
                continue;
            };
            if input_paths.contains(referenced.as_str()) {
                out.push(data_file.clone());
            }
        }
    }
    Ok(out)
}
