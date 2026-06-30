use std::future::Future;

use sea_orm::DatabaseBackend;
use sea_orm::{TransactionTrait, DbErr};
use sea_orm_migration::prelude::*;

/// Run the async closure `f` inside a single SQLite transaction and commit.
///
/// sea-orm-migration 1.x only wraps migrations in a transaction for Postgres; SQLite and MySQL
/// migrations run on the raw connection with no transaction.  For any multi-step SQLite `up()`
/// or `down()` branch that is not inherently idempotent (e.g. copy-data → drop → rename), call
/// this helper instead of `manager.get_connection().begin()` directly.
///
/// If `f` returns an error the transaction is rolled back; the migration is **not** recorded in
/// `seaql_migrations`, so it will re-run in full on the next start — making the operation safe
/// to retry.
///
/// # Example
/// ```rust
/// DatabaseBackend::Sqlite => {
///     sqlite_txn(manager, |txn| async move {
///         txn.drop_table(Table::drop().table(Foo::Table).to_owned()).await?;
///         txn.create_table(Table::create().table(Foo::Table)...to_owned()).await?;
///         Ok(())
///     }).await?;
/// }
/// ```
///
/// TODO(#26046): on sea-orm-migration ≥ 2.0, remove this helper and return
/// `Some(true)` from `MigrationTrait::use_transaction` instead.
pub async fn sqlite_txn<F, Fut>(manager: &SchemaManager<'_>, f: F) -> Result<(), DbErr>
where
    F: FnOnce(SchemaManager<'_>) -> Fut,
    Fut: Future<Output = Result<(), DbErr>>,
{
    let txn = manager.get_connection().begin().await?;
    f(SchemaManager::new(&txn)).await?;
    txn.commit().await
}

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
