/// Column family name for table.
const TABLE_CF_NAME: &str = "cf/table";
/// Column family name for schema.
const SCHEMA_CF_NAME: &str = "cf/schema";
/// Column family name for database.
const DATABASE_CF_NAME: &str = "cf/database";
/// Column family name for cluster.
const CLUSTER_CF_NAME: &str = "cf/cluster";
/// Column family name for node fragment.
const NODE_FRAGMENT_CF_NAME: &str = "cf/node_fragment";
/// Column family name for node fragment.
const TABLE_FRAGMENT_CF_NAME: &str = "cf/table_fragment";
/// Column family name for fragment node location.
const FRAGMENT_CF_NAME: &str = "cf/fragment";
/// Epoch state key, we store epoch state in default column family.
const EPOCH_STATE_KEY: &str = "epoch_state";
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
    database_cf: String,
    schema_cf: String,
    table_cf: String,

    node_fragment_cf: String,
    table_fragment_cf: String,
    fragment_cf: String,

    cluster_state_cf: String,
    epoch_state_key: String,

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
    pub fn set_table_cf(&mut self, cf: &str) {
        self.table_cf = cf.to_owned();
    }

    pub fn get_table_cf(&self) -> &str {
        self.table_cf.as_str()
    }

    pub fn set_schema_cf(&mut self, cf: &str) {
        self.schema_cf = cf.to_owned();
    }

    pub fn get_schema_cf(&self) -> &str {
        self.schema_cf.as_str()
    }

    pub fn set_database_cf(&mut self, cf: &str) {
        self.database_cf = cf.to_owned();
    }

    pub fn get_database_cf(&self) -> &str {
        self.database_cf.as_str()
    }

    pub fn set_node_fragment_cf(&mut self, cf: &str) {
        self.node_fragment_cf = cf.to_owned();
    }

    pub fn get_node_fragment_cf(&self) -> &str {
        self.node_fragment_cf.as_str()
    }

    pub fn set_fragment_cf(&mut self, cf: &str) {
        self.fragment_cf = cf.to_owned();
    }

    pub fn get_fragment_cf(&self) -> &str {
        self.fragment_cf.as_str()
    }

    pub fn set_table_fragment_cf(&mut self, cf: &str) {
        self.table_fragment_cf = cf.to_owned();
    }

    pub fn get_table_fragment_cf(&self) -> &str {
        self.table_fragment_cf.as_str()
    }

    pub fn set_cluster_cf(&mut self, cf: &str) {
        self.cluster_state_cf = cf.to_owned();
    }

    pub fn get_cluster_cf(&self) -> &str {
        self.cluster_state_cf.as_str()
    }

    pub fn set_epoch_state_cf(&mut self, cf: &str) {
        self.cluster_state_cf = cf.to_owned();
    }

    pub fn get_epoch_state_key(&self) -> &str {
        self.epoch_state_key.as_str()
    }

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
            database_cf: DATABASE_CF_NAME.to_owned(),
            schema_cf: SCHEMA_CF_NAME.to_owned(),
            table_cf: TABLE_CF_NAME.to_owned(),
            node_fragment_cf: NODE_FRAGMENT_CF_NAME.to_string(),
            table_fragment_cf: TABLE_FRAGMENT_CF_NAME.to_string(),
            epoch_state_key: EPOCH_STATE_KEY.to_owned(),
            cluster_state_cf: CLUSTER_CF_NAME.to_owned(),
            fragment_cf: FRAGMENT_CF_NAME.to_string(),
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
