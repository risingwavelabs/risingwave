#![allow(dead_code)]

/// Column family name for hummock context.
const HUMMOCK_CONTEXT_CF_NAME: &str = "cf/hummock_context";
/// Column family name for hummock version.
const HUMMOCK_VERSION_CF_NAME: &str = "cf/hummock_version";
/// Column family name for hummock table.
const HUMMOCK_TABLE_CF_NAME: &str = "cf/hummock_table";
/// Column family name for hummock epoch.
const HUMMOCK_DEFAULT_CF_NAME: &str = "cf/hummock_default";
/// Column family name for hummock deletion.
const HUMMOCK_DELETION_CF_NAME: &str = "cf/hummock_deletion";
/// Hummock version id key.
const HUMMOCK_VERSION_ID_LEY: &str = "version_id";
/// Hummock `compact_status` key
const HUMMOCK_COMPACT_STATUS_KEY: &str = "compact_status";
/// Column family name for hummock context pinned version
const HUMMOCK_CONTEXT_PINNED_VERSION_CF_NAME: &str = "cf/hummock_context_pinned_version";
/// Column family name for hummock context pinned snapshot
const HUMMOCK_CONTEXT_PINNED_SNAPSHOT_CF_NAME: &str = "cf/hummock_context_pinned_snapshot";

#[derive(Clone)]
pub struct Config {
    hummock_default_cf: String,
    hummock_context_cf: String,
    hummock_version_cf: String,
    hummock_table_cf: String,
    hummock_deletion_cf: String,
    hummock_version_id_key: String,
    hummock_compact_status_key: String,
    hummock_context_pinned_version_cf: String,
    hummock_context_pinned_snapshot_cf: String,
}

impl Config {
    pub fn get_hummock_context_cf(&self) -> &str {
        &self.hummock_context_cf
    }
    pub fn get_hummock_version_cf(&self) -> &str {
        &self.hummock_version_cf
    }
    pub fn get_hummock_table_cf(&self) -> &str {
        &self.hummock_table_cf
    }
    pub fn get_hummock_default_cf(&self) -> &str {
        &self.hummock_default_cf
    }
    pub fn get_hummock_deletion_cf(&self) -> &str {
        &self.hummock_deletion_cf
    }
    pub fn get_hummock_version_id_key(&self) -> &str {
        &self.hummock_version_id_key
    }
    pub fn get_hummock_context_pinned_version_cf(&self) -> &str {
        &self.hummock_context_pinned_version_cf
    }
    pub fn get_hummock_context_pinned_snapshot_cf(&self) -> &str {
        &self.hummock_context_pinned_snapshot_cf
    }
    pub fn get_hummock_compact_status_key(&self) -> &str {
        &self.hummock_compact_status_key
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            hummock_context_cf: HUMMOCK_CONTEXT_CF_NAME.to_owned(),
            hummock_version_cf: HUMMOCK_VERSION_CF_NAME.to_owned(),
            hummock_table_cf: HUMMOCK_TABLE_CF_NAME.to_owned(),
            hummock_default_cf: HUMMOCK_DEFAULT_CF_NAME.to_owned(),
            hummock_deletion_cf: HUMMOCK_DELETION_CF_NAME.to_owned(),
            hummock_version_id_key: HUMMOCK_VERSION_ID_LEY.to_owned(),
            hummock_compact_status_key: HUMMOCK_COMPACT_STATUS_KEY.to_owned(),
            hummock_context_pinned_version_cf: HUMMOCK_CONTEXT_PINNED_VERSION_CF_NAME.to_owned(),
            hummock_context_pinned_snapshot_cf: HUMMOCK_CONTEXT_PINNED_SNAPSHOT_CF_NAME.to_owned(),
        }
    }
}
