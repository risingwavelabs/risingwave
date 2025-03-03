use std::collections::HashMap;

use sea_orm::{FromQueryResult, Statement};
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::*;

use crate::SubQueryStatement::SelectStatement;
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(StreamingJob::Table)
                    .add_column(ColumnDef::new(StreamingJob::ExprContext).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        fulfill_streaming_job_expr_context(manager).await?;

        manager
            .alter_table(
                Table::alter()
                    .table(Actor::Table)
                    .drop_column(Actor::ExprContext)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // manager
        //     .alter_table(
        //         Table::alter()
        //             .table(Fragment::Table)
        //             .drop_column(Fragment::ExprContext)
        //             .to_owned(),
        //     )
        //     .await?;
        //
        // Ok(())
        unimplemented!()
    }
}

// Fulfill the FragmentExprContext table with data from the Actor and ActorDispatcher tables
async fn fulfill_streaming_job_expr_context(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let connection = manager.get_connection();

    let database_backend = connection.get_database_backend();

    // let (sql, values) = Query::select()
    //     .distinct()
    //     .expr_as(
    //         Expr::col((Actor::Table, Actor::FragmentId)),
    //         Fragment::FragmentId,
    //     )
    //     .expr_as(
    //         Expr::col((Actor::Table, Actor::ExprContext)),
    //         Fragment::ExprContext,
    //     )
    //     .columns([
    //         (Actor::Table, Actor::FragmentId),
    //         (Actor::Table, Actor::ExprContext),
    //     ])
    //     .from(Actor::Table)
    //     .to_owned()
    //     .build_any(&*database_backend.get_query_builder());
    //
    // let rows = connection
    //     .query_all(Statement::from_sql_and_values(
    //         database_backend,
    //         sql,
    //         values,
    //     ))
    //     .await?;
    //
    // let mut expr_contexts_by_fragment = HashMap::new();
    //
    // for row in rows {
    //     let FragmentExprContextEntity {
    //         fragment_id,
    //         expr_context,
    //     } = FragmentExprContextEntity::from_query_result(&row, "")?;
    //
    //     if let Some(prev) = expr_contexts_by_fragment.get(&fragment_id) {
    //         assert_eq!(prev, &expr_context);
    //     }
    //
    //     expr_contexts_by_fragment.insert(fragment_id, expr_context);
    // }

    let stmt = Query::update()
        .table(StreamingJob::Table)
        .value(
            StreamingJob::ExprContext,
            SimpleExpr::SubQuery(
                None,
                Box::new(SelectStatement(
                    Query::select()
                        .distinct()
                        .column((Actor::Table, Actor::ExprContext))
                        .from(Actor::Table)
                        .join(
                            JoinType::InnerJoin,
                            Fragment::Table,
                            Expr::col((Actor::Table, Actor::FragmentId))
                                .equals((Fragment::Table, Fragment::FragmentId)),
                        )
                        .and_where(
                            Expr::col((StreamingJob::Table, StreamingJob::JobId))
                                .equals((Fragment::Table, Fragment::JobId)),
                        )
                        .to_owned(),
                )),
            ),
        )
        .and_where(Expr::col((StreamingJob::Table, StreamingJob::ExprContext)).is_null())
        .to_owned();

    manager.exec_stmt(stmt).await?;

    Ok(())
}

#[derive(DeriveIden)]
enum StreamingJob {
    Table,
    JobId,
    ExprContext,
}

#[derive(DeriveIden)]
enum Actor {
    Table,
    ActorId,
    FragmentId,
    ExprContext,
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    JobId,
}

#[derive(Debug, FromQueryResult)]
#[sea_orm(entity = "Fragment")]
pub struct FragmentExprContextEntity {
    fragment_id: i32,
    expr_context: Vec<u8>,
}
