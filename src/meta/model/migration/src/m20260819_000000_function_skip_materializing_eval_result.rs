use sea_orm_migration::prelude::{Table as MigrationTable, *};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .add_column(
                        ColumnDef::new(Function::SkipMaterializingEvalResult)
                            .boolean()
                            .not_null()
                            .default(false),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        assert!(manager.has_table(Function::Table.to_string()).await?);

        manager
            .alter_table(
                MigrationTable::alter()
                    .table(Function::Table)
                    .drop_column(Function::SkipMaterializingEvalResult)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(DeriveIden)]
enum Function {
    Table,
    SkipMaterializingEvalResult,
}

#[cfg(test)]
mod tests {
    use sea_orm::{ConnectionTrait, Database, DatabaseBackend, Statement, TryGetable};

    use super::*;

    #[tokio::test]
    async fn test_adds_skip_materializing_eval_result_with_false_default() {
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

        assert!(
            manager
                .has_column("function", "skip_materializing_eval_result")
                .await
                .unwrap()
        );
        let existing_function = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT CAST("skip_materializing_eval_result" AS INTEGER) AS "skip_materializing_eval_result" FROM "function" WHERE "function_id" = 1"#,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::try_get(&existing_function, "", "skip_materializing_eval_result").unwrap(),
            0
        );

        db.execute(Statement::from_string(
            DatabaseBackend::Sqlite,
            r#"INSERT INTO "function" ("function_id", "skip_materializing_eval_result") VALUES (2, TRUE)"#,
        ))
        .await
        .unwrap();
        let opted_out_function = db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                r#"SELECT CAST("skip_materializing_eval_result" AS INTEGER) AS "skip_materializing_eval_result" FROM "function" WHERE "function_id" = 2"#,
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            i32::try_get(&opted_out_function, "", "skip_materializing_eval_result").unwrap(),
            1
        );

        Migration.down(&manager).await.unwrap();
        assert!(
            !manager
                .has_column("function", "skip_materializing_eval_result")
                .await
                .unwrap()
        );
    }
}
