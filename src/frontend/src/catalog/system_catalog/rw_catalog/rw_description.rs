use std::iter;

use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_DESCRIPTION: BuiltinTable = BuiltinTable {
    name: "rw_description",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "objoid"),
        (DataType::Int32, "classoid"),
        (DataType::Int32, "objsubid"),
        (DataType::Varchar, "description"),
    ],
    pk: &[0, 1, 2],
};

impl SysCatalogReaderImpl {
    pub fn read_rw_description(&self) -> Result<Vec<OwnedRow>> {
        let build_row = |table_id, catalog_id, description| {
            OwnedRow::new(vec![
                Some(ScalarImpl::Int32(table_id)),
                Some(ScalarImpl::Int32(catalog_id)),
                Some(ScalarImpl::Int32(0)),
                Some(ScalarImpl::Utf8(description)),
            ])
        };
        let build_row_with_sub = |table_id, catalog_id, index, description| {
            OwnedRow::new(vec![
                Some(ScalarImpl::Int32(table_id)),
                Some(ScalarImpl::Int32(catalog_id)),
                Some(ScalarImpl::Int32(index)),
                Some(ScalarImpl::Utf8(description)),
            ])
        };

        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas_except_rw_catalog(&self.auth_context.database)?;
        let rw_catalog = reader.get_schema_by_name(&self.auth_context.database, "rw_catalog")?;

        // XXX: is it shared object ??
        let database_desc = reader.iter_databases().map(|db| {
            build_row(
                db.id() as _,
                rw_catalog
                    .get_system_table_by_name("rw_databases")
                    .map(|st| st.id.table_id)
                    .unwrap_or_default() as _,
                db.description().unwrap_or_default().into(),
            )
        });

        Ok(schemas
            .flat_map(|schema| {
                let table_desc = schema.iter_table().flat_map(|table| {
                    iter::once(build_row(
                        table.id.table_id as _,
                        rw_catalog
                            .get_system_table_by_name("rw_tables")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        table.description.as_deref().unwrap_or_default().into(),
                    ))
                    .chain(
                        table
                            .columns
                            .iter()
                            .filter(|col| !col.is_hidden())
                            .map(|col| {
                                build_row_with_sub(
                                    table.id.table_id as _,
                                    rw_catalog
                                        .get_system_table_by_name("rw_tables")
                                        .map(|st| st.id.table_id)
                                        .unwrap_or_default()
                                        as _,
                                    col.column_id().get_id() as _,
                                    col.column_desc
                                        .description
                                        .as_deref()
                                        .unwrap_or_default()
                                        .into(),
                                )
                            }),
                    )
                });
                let mv_desc = schema.iter_mv().map(|mv| {
                    build_row(
                        mv.id.table_id as _,
                        rw_catalog
                            .get_system_table_by_name("rw_materialized_views")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        mv.description.as_deref().unwrap_or_default().into(),
                    )
                });
                let index_desc = schema.iter_index().map(|index| {
                    build_row(
                        index.id.index_id as _,
                        rw_catalog
                            .get_system_table_by_name("rw_indexes")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        index.description.as_deref().unwrap_or_default().into(),
                    )
                });
                let source_desc = schema.iter_source().map(|source| {
                    build_row(
                        source.id as _,
                        rw_catalog
                            .get_system_table_by_name("rw_sources")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        source.description.as_deref().unwrap_or_default().into(),
                    )
                });
                let sink_desc = schema.iter_sink().map(|sink| {
                    build_row(
                        sink.id.sink_id as _,
                        rw_catalog
                            .get_system_table_by_name("rw_sinks")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        sink.description.as_deref().unwrap_or_default().into(),
                    )
                });
                let view_desc = schema.iter_view().map(|view| {
                    build_row(
                        view.id as _,
                        rw_catalog
                            .get_system_table_by_name("rw_views")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        view.description.as_deref().unwrap_or_default().into(),
                    )
                });
                let function_desc = schema.iter_function().map(|function| {
                    build_row(
                        function.id.function_id() as _,
                        rw_catalog
                            .get_system_table_by_name("rw_functions")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        function.description.as_deref().unwrap_or_default().into(),
                    )
                });
                let connection_desc = schema.iter_connections().map(|connection| {
                    build_row(
                        connection.id as _,
                        rw_catalog
                            .get_system_table_by_name("rw_connetions")
                            .map(|st| st.id.table_id)
                            .unwrap_or_default() as _,
                        connection.description.as_deref().unwrap_or_default().into(),
                    )
                });

                table_desc
                    .chain(mv_desc)
                    .chain(index_desc)
                    .chain(source_desc)
                    .chain(sink_desc)
                    .chain(view_desc)
                    .chain(function_desc)
                    .chain(connection_desc)
            })
            .chain(database_desc)
            .collect())
    }
}
