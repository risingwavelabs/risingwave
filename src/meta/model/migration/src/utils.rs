use sea_orm::DatabaseBackend;
use sea_orm_migration::prelude::*;

#[easy_ext::ext(ColumnDefExt)]
impl ColumnDef {
    /// Set column type as `longblob` for MySQL, `bytea` for Postgres, and `blob` for Sqlite.
    pub fn rw_binary(&mut self, manager: &SchemaManager) -> &mut Self {
        match manager.get_database_backend() {
            DatabaseBackend::MySql => self.custom(extension::mysql::MySqlType::LongBlob),
            DatabaseBackend::Postgres | DatabaseBackend::Sqlite => self.blob(),
        }
    }
}
