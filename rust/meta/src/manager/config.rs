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
        }
    }
}
