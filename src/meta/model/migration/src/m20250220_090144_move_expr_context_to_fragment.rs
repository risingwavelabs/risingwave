use sea_orm::{FromQueryResult, Statement};
use sea_orm_migration::prelude::*;
use sea_orm_migration::schema::*;

use crate::m20250106_072104_fragment_relation::{FragmentRelationEntity, I32Array};
use crate::utils::ColumnDefExt;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Fragment::Table)
                    .add_column(ColumnDef::new(Fragment::ExprContext).rw_binary(manager))
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Replace the sample below with your own migration scripts
        todo!();

        manager
            .drop_table(Table::drop().table(Post::Table).to_owned())
            .await
    }
}

// Fulfill the FragmentRelation table with data from the Actor and ActorDispatcher tables
async fn fulfill_fragment_expr_context(manager: &SchemaManager<'_>) -> Result<(), DbErr> {
    let connection = manager.get_connection();

    let database_backend = connection.get_database_backend();

    let (sql, values) = Query::select()
        // as sqlite does not support `distinct on` yet, we have to use `distinct` here
        .distinct()
        // .expr_as(
        //     Expr::col((Actor::Table, Actor::FragmentId)),
        //     crate::m20250106_072104_fragment_relation::FragmentRelation::SourceFragmentId,
        // )
        // .expr_as(
        //     Expr::col((crate::m20250106_072104_fragment_relation::ActorDispatcher::Table, crate::m20250106_072104_fragment_relation::ActorDispatcher::DispatcherId)),
        //     crate::m20250106_072104_fragment_relation::FragmentRelation::TargetFragmentId,
        // )
        .columns([
            (Actor::Table, Actor::FragmentId),
            (Actor::Table, Actor::ExprContext),
        ])
        .from(Actor::Table)
        // .join(
        //     JoinType::InnerJoin,
        //     crate::m20250106_072104_fragment_relation::ActorDispatcher::Table,
        //     Expr::col((Actor::Table, Actor::ActorId))
        //         .equals((crate::m20250106_072104_fragment_relation::ActorDispatcher::Table, crate::m20250106_072104_fragment_relation::ActorDispatcher::ActorId)),
        // )
        .to_owned()
        .build_any(&*database_backend.get_query_builder());

    let rows = connection
        .query_all(Statement::from_sql_and_values(
            database_backend,
            sql,
            values,
        ))
        .await?;

    for row in rows {
        // let FragmentRelationEntity {
        //     source_fragment_id,
        //     target_fragment_id,
        //     dispatcher_type,
        //     dist_key_indices,
        //     output_indices,
        // } = FragmentRelationEntity::from_query_result(&row, "")?;

        manager
            .exec_stmt(
                Query::insert()
                    .into_table(crate::m20250106_072104_fragment_relation::FragmentRelation::Table)
                    .columns([
                        crate::m20250106_072104_fragment_relation::FragmentRelation::SourceFragmentId,
                        crate::m20250106_072104_fragment_relation::FragmentRelation::TargetFragmentId,
                        crate::m20250106_072104_fragment_relation::FragmentRelation::DispatcherType,
                        crate::m20250106_072104_fragment_relation::FragmentRelation::DistKeyIndices,
                        crate::m20250106_072104_fragment_relation::FragmentRelation::OutputIndices,
                    ])
                    .values_panic([
                        source_fragment_id.into(),
                        target_fragment_id.into(),
                        dispatcher_type.into(),
                        dist_key_indices.into(),
                        output_indices.into(),
                    ])
                    .to_owned(),
            )
            .await?;
    }

    Ok(())
}

#[derive(DeriveIden)]
enum Fragment {
    Table,
    FragmentId,
    ExprContext,
}

#[derive(DeriveIden)]
enum Actor {
    Table,
    ActorId,
    FragmentId,
    ExprContext,
}

// #[derive(Debug, FromQueryResult)]
// #[sea_orm(entity = "Fragment")]
// pub struct FragmentExprContextEntity {
//     fragment_id: i32,
//     expr_context: Vec<u8>,
// }
