// Copyright 2023 RisingWave Labs
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

use std::collections::BTreeSet;

/// Extracts `pub mod xxx;` module names from `risingwave_meta_model/src/lib.rs`.
fn parse_pub_mods(lib_rs: &str) -> BTreeSet<String> {
    let mut mods = BTreeSet::new();
    for line in lib_rs.lines() {
        let line = line.trim();
        let Some(rest) = line.strip_prefix("pub mod ") else {
            continue;
        };
        let rest = rest.trim();
        let Some(name) = rest.strip_suffix(';') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            mods.insert(name.to_owned());
        }
    }
    mods
}

/// Extracts `risingwave_meta_model::xxx` module names referenced in `for_all_metadata_models_v2`.
fn parse_models_in_for_all_metadata_models_v2(meta_snapshot_v2_rs: &str) -> BTreeSet<String> {
    let mut mods = BTreeSet::new();
    for line in meta_snapshot_v2_rs.lines() {
        let needle = "risingwave_meta_model::";
        let mut s = line;
        while let Some(idx) = s.find(needle) {
            let after = &s[idx + needle.len()..];
            let ident: String = after
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect();
            let ident_len = ident.len();
            if !ident.is_empty() {
                mods.insert(ident);
            }
            s = &after[ident_len..];
        }
    }
    mods
}

#[test]
fn for_all_metadata_models_v2_should_cover_all_required_meta_model_tables() {
    // NOTE: This test is intentionally "source-based" to act as a CI guard.
    //
    // When a new metadata table is added to `risingwave_meta_model` (i.e., a new `pub mod xxx;`),
    // we should decide whether it needs to be included in meta snapshot v2.
    //
    // - If yes: append it to `for_all_metadata_models_v2` in
    //   `src/storage/backup/src/meta_snapshot_v2.rs`.
    // - If no (runtime/derived table): add it to the `excluded` set below with a short comment.

    let meta_model_lib_rs = include_str!("../../../meta/model/src/lib.rs");
    let meta_snapshot_v2_rs = include_str!("../src/meta_snapshot_v2.rs");

    let all_meta_model_modules = parse_pub_mods(meta_model_lib_rs);

    // Tables/modules that are intentionally NOT included in meta snapshot v2.
    // These are runtime / derived states that should not be backed up, or are handled separately.
    let excluded: BTreeSet<&'static str> = [
        // Not a metadata table.
        "prelude",
        // Runtime compaction states/tasks.
        "compaction_status",
        "compaction_task",
        // Hummock runtime/derived tables.
        "hummock_epoch_to_version",
        "hummock_gc_history",
        "hummock_pinned_snapshot",
        "hummock_pinned_version",
        "hummock_sstable_info",
        "hummock_time_travel_delta",
        "hummock_time_travel_version",
        // Version deltas are replayed from meta store during snapshot build, not persisted in snapshot.
        "hummock_version_delta",
    ]
    .into_iter()
    .collect();

    let expected: BTreeSet<String> = all_meta_model_modules
        .into_iter()
        .filter(|m| !excluded.contains(m.as_str()))
        .collect();

    let actual = parse_models_in_for_all_metadata_models_v2(meta_snapshot_v2_rs);

    let missing: Vec<_> = expected.difference(&actual).cloned().collect();
    let unexpected: Vec<_> = actual.difference(&expected).cloned().collect();

    assert!(
        missing.is_empty() && unexpected.is_empty(),
        "for_all_metadata_models_v2 is out of sync with risingwave_meta_model.\n\
Missing in for_all_metadata_models_v2: {missing:?}\n\
Unexpected in for_all_metadata_models_v2: {unexpected:?}\n\
Hint: update `src/storage/backup/src/meta_snapshot_v2.rs` or the `excluded` list in this test."
    );
}
