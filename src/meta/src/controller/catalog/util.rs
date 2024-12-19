use super::*;

pub(crate) async fn update_internal_tables(
    txn: &DatabaseTransaction,
    object_id: i32,
    column: object::Column,
    new_value: Value,
    relations_to_notify: &mut Vec<PbRelationInfo>,
) -> MetaResult<()> {
    let internal_tables = get_internal_tables_by_id(object_id, txn).await?;

    if !internal_tables.is_empty() {
        Object::update_many()
            .col_expr(column, SimpleExpr::Value(new_value))
            .filter(object::Column::Oid.is_in(internal_tables.clone()))
            .exec(txn)
            .await?;

        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(internal_tables))
            .all(txn)
            .await?;
        for (table, table_obj) in table_objs {
            relations_to_notify.push(PbRelationInfo::Table(
                ObjectModel(table, table_obj.unwrap()).into(),
            ));
        }
    }
    Ok(())
}

impl CatalogController {
    pub(crate) async fn init(&self) -> MetaResult<()> {
        self.table_catalog_cdc_table_id_update().await?;
        Ok(())
    }

    /// Fill in the `cdc_table_id` field for Table with empty `cdc_table_id` and parent Source job.
    /// NOTES: We assume Table with a parent Source job is a CDC table
    pub(crate) async fn table_catalog_cdc_table_id_update(&self) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        // select Tables which cdc_table_id is empty and has a parent Source job
        let table_and_source_id: Vec<(TableId, String, SourceId)> = Table::find()
            .join(JoinType::InnerJoin, table::Relation::ObjectDependency.def())
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Source.def(),
            )
            .select_only()
            .columns([table::Column::TableId, table::Column::Definition])
            .columns([source::Column::SourceId])
            .filter(
                table::Column::TableType.eq(TableType::Table).and(
                    table::Column::CdcTableId
                        .is_null()
                        .or(table::Column::CdcTableId.eq("")),
                ),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        // return directly if the result set is empty.
        if table_and_source_id.is_empty() {
            return Ok(());
        }

        info!(table_and_source_id = ?table_and_source_id, "cdc table with empty cdc_table_id");

        let mut cdc_table_ids = HashMap::new();
        for (table_id, definition, source_id) in table_and_source_id {
            match extract_external_table_name_from_definition(&definition) {
                None => {
                    tracing::warn!(
                        table_id = table_id,
                        definition = definition,
                        "failed to extract cdc table name from table definition.",
                    )
                }
                Some(external_table_name) => {
                    cdc_table_ids.insert(
                        table_id,
                        build_cdc_table_id(source_id as u32, &external_table_name),
                    );
                }
            }
        }

        for (table_id, cdc_table_id) in cdc_table_ids {
            table::ActiveModel {
                table_id: Set(table_id as _),
                cdc_table_id: Set(Some(cdc_table_id)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn list_object_dependencies(
        &self,
        include_creating: bool,
    ) -> MetaResult<Vec<PbObjectDependencies>> {
        let inner = self.inner.read().await;

        let dependencies: Vec<(ObjectId, ObjectId)> = {
            let filter = if include_creating {
                Expr::value(true)
            } else {
                streaming_job::Column::JobStatus.eq(JobStatus::Created)
            };
            ObjectDependency::find()
                .select_only()
                .columns([
                    object_dependency::Column::Oid,
                    object_dependency::Column::UsedBy,
                ])
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        let mut obj_dependencies = dependencies
            .into_iter()
            .map(|(oid, used_by)| PbObjectDependencies {
                object_id: used_by as _,
                referenced_object_id: oid as _,
            })
            .collect_vec();

        let view_dependencies: Vec<(ObjectId, ObjectId)> = ObjectDependency::find()
            .select_only()
            .columns([
                object_dependency::Column::Oid,
                object_dependency::Column::UsedBy,
            ])
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Object1.def(),
            )
            .join(JoinType::InnerJoin, object::Relation::View.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        obj_dependencies.extend(view_dependencies.into_iter().map(|(view_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id as _,
                referenced_object_id: view_id as _,
            }
        }));

        let sink_dependencies: Vec<(SinkId, TableId)> = {
            let filter = if include_creating {
                sink::Column::TargetTable.is_not_null()
            } else {
                streaming_job::Column::JobStatus
                    .eq(JobStatus::Created)
                    .and(sink::Column::TargetTable.is_not_null())
            };
            Sink::find()
                .select_only()
                .columns([sink::Column::SinkId, sink::Column::TargetTable])
                .join(JoinType::InnerJoin, sink::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(sink_dependencies.into_iter().map(|(sink_id, table_id)| {
            PbObjectDependencies {
                object_id: table_id as _,
                referenced_object_id: sink_id as _,
            }
        }));

        let subscription_dependencies: Vec<(SubscriptionId, TableId)> = {
            let filter = if include_creating {
                subscription::Column::DependentTableId.is_not_null()
            } else {
                subscription::Column::SubscriptionState
                    .eq(Into::<i32>::into(SubscriptionState::Created))
                    .and(subscription::Column::DependentTableId.is_not_null())
            };
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::DependentTableId,
                ])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .filter(filter)
                .into_tuple()
                .all(&inner.db)
                .await?
        };
        obj_dependencies.extend(subscription_dependencies.into_iter().map(
            |(subscription_id, table_id)| PbObjectDependencies {
                object_id: subscription_id as _,
                referenced_object_id: table_id as _,
            },
        ));

        Ok(obj_dependencies)
    }
}
