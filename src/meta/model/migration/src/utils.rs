use sea_orm::DatabaseBackend;
use sea_orm_migration::prelude::*;

#[easy_ext::ext(ColumnDefExt)]
impl ColumnDef {
    /// Set column type as `longblob` for MySQL, `bytea` for Postgres, and `blob` for Sqlite.
    ///
    /// Should be preferred over [`binary`](ColumnDef::binary) or [`blob`](ColumnDef::blob) for large binary fields,
    /// typically the fields wrapping protobuf or other serialized data. Otherwise, MySQL will return an error
    /// when the length exceeds 65535 bytes.
    pub fn rw_binary(&mut self, manager: &SchemaManager) -> &mut Self {
        match manager.get_database_backend() {
            DatabaseBackend::MySql => self.custom(extension::mysql::MySqlType::LongBlob),
            #[expect(clippy::disallowed_methods)]
            DatabaseBackend::Postgres | DatabaseBackend::Sqlite => self.blob(),
        }
    }

    /// Set column type as `longtext` for MySQL, and `text` for Postgres and Sqlite.
    ///
    /// Should be preferred over [`text`](ColumnDef::text) or [`string`](ColumnDef::string) for large text fields,
    /// typically user-specified contents like UDF body or SQL definition. Otherwise, MySQL will return an error
    /// when the length exceeds 65535 bytes.
    pub fn rw_long_text(&mut self, manager: &SchemaManager) -> &mut Self {
        match manager.get_database_backend() {
            DatabaseBackend::MySql => self.custom(Alias::new("longtext")),
            DatabaseBackend::Postgres | DatabaseBackend::Sqlite => self.text(),
        }
    }
}
