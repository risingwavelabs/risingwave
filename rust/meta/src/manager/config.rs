#![allow(dead_code)]



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
