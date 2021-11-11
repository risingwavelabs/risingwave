/// Column family name for column table.
const COLUMN_TABLE_CF_NAME: &str = "cf(column)/table";
/// Column family name for row table.
const ROW_TABLE_CF_NAME: &str = "cf(row)/table";
/// Column family name for schema.
const SCHEMA_CF_NAME: &str = "cf/schema";
/// Column family name for database.
const DATABASE_CF_NAME: &str = "cf/database";
/// Column family name for cluster.
const CLUSTER_CF_NAME: &str = "cf/cluster";

/// Epoch state key, we store epoch state in default column family.
const EPOCH_STATE_KEY: &str = "epoch_state";

#[derive(Clone)]
pub struct Config {
    database_cf: String,
    schema_cf: String,

    column_table_cf: String,
    row_table_cf: String,

    cluster_state_cf: String,
    epoch_state_key: String,
}

impl Config {
    pub fn set_column_table_cf(&mut self, cf: &str) {
        self.column_table_cf = cf.to_owned();
    }

    pub fn get_column_table_cf(&self) -> &str {
        self.column_table_cf.as_str()
    }

    pub fn set_row_table_cf(&mut self, cf: &str) {
        self.row_table_cf = cf.to_owned();
    }

    pub fn get_row_table_cf(&self) -> &str {
        self.row_table_cf.as_str()
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
            column_table_cf: COLUMN_TABLE_CF_NAME.to_owned(),
            row_table_cf: ROW_TABLE_CF_NAME.to_owned(),
            epoch_state_key: EPOCH_STATE_KEY.to_owned(),
            cluster_state_cf: CLUSTER_CF_NAME.to_owned(),
        }
    }
}
