use sea_orm::DbBackend;
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // existing mysql backend may using `utf8mb3` charset, let's ensure it's `utf8mb4` after this migration
        if manager.get_database_backend() == DbBackend::MySql {
            manager
                .get_connection()
                .execute_unprepared("ALTER DATABASE CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
                .await
                .expect("failed to set database collate");
        }
        Ok(())
    }

    async fn down(&self, _manager: &SchemaManager) -> Result<(), DbErr> {
        // do nothing
        Ok(())
    }
}
