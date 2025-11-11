use std::str::FromStr as _;

use anyhow::Context as _;
use serde::Serialize;
use serde::de::DeserializeOwned;
use toml::map::Entry;
use toml::{Table, Value};

def_anyhow_newtype! { pub ConfigMergeError }

/// Extract the section at `partial_path` from `partial`, merge it into `base` to override entries.
///
/// Tables will be merged recursively, while other fields (including arrays) will be replaced by
/// the `partial` config, if exists.
///
/// Returns an error if any of the input is invalid, or the merged config cannot be parsed.
/// Returns `None` if there's nothing to override.
pub fn merge_config<C: Serialize + DeserializeOwned + Clone>(
    base: &C,
    partial: &str,
    partial_path: impl IntoIterator<Item = &str>,
) -> Result<Option<C>, ConfigMergeError> {
    let partial_table = {
        let mut partial_table =
            Table::from_str(partial).context("failed to parse partial config")?;
        for k in partial_path {
            if let Some(v) = partial_table.remove(k)
                && let Value::Table(t) = v
            {
                partial_table = t;
            } else {
                // The section to override is not relevant.
                return Ok(None);
            }
        }
        partial_table
    };

    if partial_table.is_empty() {
        // Nothing to override.
        return Ok(None);
    }

    let mut base_table = Table::try_from(base).context("failed to serialize base config")?;

    fn merge_table(base_table: &mut Table, partial_table: Table) {
        for (k, v) in partial_table {
            match base_table.entry(k) {
                Entry::Vacant(entry) => {
                    // Unrecognized entry might be tolerated.
                    // So we simply keep it and postpone the error (if any) to final deserialization.
                    entry.insert(v);
                }
                Entry::Occupied(mut entry) => {
                    let base_v = entry.get_mut();
                    merge_value(base_v, v);
                }
            }
        }
    }

    fn merge_value(base: &mut Value, partial: Value) {
        if let Value::Table(base_table) = base
            && let Value::Table(partial_table) = partial
        {
            merge_table(base_table, partial_table);
        } else {
            *base = partial;
        }
    }

    merge_table(&mut base_table, partial_table);

    let merged: C = base_table
        .try_into()
        .context("failed to deserialize merged config")?;

    Ok(Some(merged))
}

#[cfg(test)]
mod tests {
    use thiserror_ext::AsReport;

    use super::*;
    use crate::config::StreamingConfig;

    #[test]
    fn test_merge_streaming_config() {
        let base = StreamingConfig::default();
        assert_ne!(base.unsafe_enable_strict_consistency, false);
        assert_ne!(base.developer.chunk_size, 114514);
        assert_ne!(
            base.developer.compute_client_config.connect_timeout_secs,
            114514
        );

        let partial = r#"
            [streaming]
            unsafe_enable_strict_consistency = false

            [streaming.developer]
            stream_chunk_size = 114514
            stream_compute_client_config = { connect_timeout_secs = 114514 }
        "#;
        let merged = merge_config(&base, partial, ["streaming"])
            .unwrap()
            .unwrap();

        // Demonstrate that the entries are merged.
        assert_eq!(merged.unsafe_enable_strict_consistency, false);
        assert_eq!(merged.developer.chunk_size, 114514);
        assert_eq!(
            merged.developer.compute_client_config.connect_timeout_secs,
            114514
        );

        // Demonstrate that the rest of the config is not affected.
        {
            let mut merged = merged;
            merged.unsafe_enable_strict_consistency = base.unsafe_enable_strict_consistency;
            merged.developer.chunk_size = base.developer.chunk_size;
            merged.developer.compute_client_config.connect_timeout_secs =
                base.developer.compute_client_config.connect_timeout_secs;

            pretty_assertions::assert_eq!(format!("{base:?}"), format!("{merged:?}"));
        }
    }

    #[test]
    fn test_nothing_to_override() {
        let base = StreamingConfig::default();
        let partial = r#"
            [batch.developer]
            batch_chunk_size = 114514
        "#;
        let merged = merge_config(&base, partial, ["streaming"]).unwrap();
        assert!(
            merged.is_none(),
            "nothing to override, but got: {merged:#?}"
        );
    }

    #[test]
    fn test_unrecognized_entry() {
        let base = StreamingConfig::default();
        let partial = r#"
            [streaming]
            no_such_entry = 114514
        "#;
        let merged = merge_config(&base, partial, ["streaming"])
            .unwrap()
            .unwrap();

        let unrecognized = merged.unrecognized.into_inner();
        assert_eq!(unrecognized.len(), 1);
        assert_eq!(unrecognized["no_such_entry"], 114514);
    }

    #[test]
    fn test_invalid_type() {
        let base = StreamingConfig::default();
        let partial = r#"
            [streaming.developer]
            stream_chunk_size = "omakase"
        "#;
        let error = merge_config(&base, partial, ["streaming"]).unwrap_err();
        expect_test::expect![[r#"
            failed to deserialize merged config: invalid type: string "omakase", expected usize
            in `developer.stream_chunk_size`
        "#]]
        .assert_eq(&error.to_report_string());
    }
}
