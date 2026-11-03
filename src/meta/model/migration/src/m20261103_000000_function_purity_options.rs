use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

const COLUMNS: [Function; 2] = [Function::UnsafeSkipMaterializingExprs, Function::IsImmutable];

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        for column in COLUMNS {
            manager
                .alter_table(
                    MigrationTable::alter()
                        .table(Function::Table)
                        .add_column(ColumnDef::new(column).boolean().not_null().default(false))
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        for column in COLUMNS {
            manager
                .alter_table(
                    MigrationTable::alter()
                        .table(Function::Table)
                        .drop_column(column)
                        .to_owned(),
                )
                .await?;
        }

        Ok(())
    }
}

#[derive(DeriveIden, Clone, Copy)]
enum Function {
    Table,
    UnsafeSkipMaterializingExprs,
    IsImmutable,
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TryGetable};

    use super::*;

    #[tokio::test]
    async fn test_adds_purity_options_with_false_default() {
        let db = Database::connect("sqlite::memory:").await.unwrap();
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"CREATE TABLE "function" ("function_id" INTEGER PRIMARY KEY)"#,
        ))
        .await
        .unwrap();
        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"INSERT INTO "function" ("function_id") VALUES (1)"#,
        ))
        .await
        .unwrap();

        let manager = SchemaManager::new(&db);
        Migration.up(&manager).await.unwrap();

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"INSERT INTO "function" ("function_id", "unsafe_skip_materializing_exprs", "is_immutable") VALUES (2, TRUE, TRUE)"#,
        ))
        .await
        .unwrap();

        for column in ["unsafe_skip_materializing_exprs", "is_immutable"] {
            assert!(manager.has_column("function", column).await.unwrap());
            for (function_id, expected) in [(1, 0), (2, 1)] {
                let row = db
                    .query_one(Statement::from_string(
                        DatabaseBackend::Sqlite,
                        format!(
                            r#"SELECT CAST("{column}" AS INTEGER) AS "{column}" FROM "function" WHERE "function_id" = {function_id}"#
                        ),
                    ))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(i32::try_get(&row, "", column).unwrap(), expected);
            }
        }

        Migration.down(&manager).await.unwrap();
        for column in ["unsafe_skip_materializing_exprs", "is_immutable"] {
            assert!(!manager.has_column("function", column).await.unwrap());
        }
    }
}
