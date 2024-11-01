use sea_orm_migration::prelude::*;
use serde::{Deserialize, Serialize};

use crate::sea_orm::{FromJsonQueryResult, FromQueryResult, Statement};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .add_column(ColumnDef::new(WorkerProperty::Parallelism).integer())
                    .to_owned(),
            )
            .await?;

        set_worker_parallelism(manager).await?;

        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .drop_column(WorkerProperty::ParallelUnitIds)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Fragment::Table)
                    .drop_column(Fragment::VnodeMapping)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Actor::Table)
                    .drop_column(Actor::ParallelUnitId)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .drop_column(WorkerProperty::Parallelism)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(WorkerProperty::Table)
                    .add_column(
                        ColumnDef::new(WorkerProperty::ParallelUnitIds)
                            .json_binary()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Fragment::Table)
                    .add_column(
                        ColumnDef::new(Fragment::VnodeMapping)
                            .rw_binary(manager)
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Actor::Table)
                    .add_column(ColumnDef::new(Actor::ParallelUnitId).integer().not_null())
                    .to_owned(),
            )
            .await
    }
}

// Set worker parallelism based on the number of parallel unit ids
async fn set_worker_parallelism(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let connection = manager.get_connection();

    let database_backend = connection.get_database_backend();

    let (sql, values) = Query::select()
        .columns([
            (WorkerProperty::Table, WorkerProperty::WorkerId),
            (WorkerProperty::Table, WorkerProperty::ParallelUnitIds),
        ])
        .from(WorkerProperty::Table)
        .to_owned()
        .build_any(&*database_backend.get_query_builder());

    let stmt = Statement::from_sql_and_values(database_backend, sql, values);

    for WorkerPropertyParallelUnitIds {
        worker_id,
        parallel_unit_ids,
    } in WorkerPropertyParallelUnitIds::find_by_statement(stmt)
        .all(connection)
        .await?
    {
        manager
            .exec_stmt(
                Query::update()
                    .table(WorkerProperty::Table)
                    .value(
                        WorkerProperty::Parallelism,
                        Expr::value(parallel_unit_ids.0.len() as i32),
                    )
                    .and_where(Expr::col(WorkerProperty::WorkerId).eq(worker_id))
                    .to_owned(),
            )
            .await?;
    }
    Ok(())
}
#[derive(Clone, Debug, PartialEq, FromJsonQueryResult, Serialize, Deserialize, Default)]
pub struct I32Array(pub Vec<i32>);

#[derive(Debug, FromQueryResult)]
pub struct WorkerPropertyParallelUnitIds {
    worker_id: i32,
    parallel_unit_ids: I32Array,
}

#[derive(DeriveIden)]
enum WorkerProperty {
    Table,
    WorkerId,
    Parallelism,
    ParallelUnitIds,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    VnodeMapping,
}

#[derive(DeriveIden)]
enum Actor {
    Table,
    ParallelUnitId,
}
