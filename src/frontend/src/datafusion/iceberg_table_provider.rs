// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Fields as ArrowFields, Schema as ArrowSchema};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::*;
use risingwave_common::array::ArrayError;
use risingwave_common::array::arrow::IcebergArrowConvert;
use risingwave_connector::source::ConnectorProperties;
use risingwave_connector::source::iceberg::IcebergProperties;
use risingwave_pb::batch_plan::iceberg_scan_node::IcebergScanType;

use super::IcebergScan;
use crate::error::{ErrorCode, Result as RwResult, RwError};
use crate::optimizer::plan_node::LogicalIcebergScan;

/// A table provider for iceberg file scan tasks
///
/// It holds the necessary properties and schema to facilitate scanning Iceberg tables.
#[derive(Debug, Clone)]
pub struct IcebergTableProvider {
    pub iceberg_properties: Arc<IcebergProperties>,
    pub arrow_schema: Arc<ArrowSchema>,
    pub snapshot_id: Option<i64>,
    pub iceberg_scan_type: IcebergScanType,
}

#[async_trait]
impl TableProvider for IcebergTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(IcebergScan::new(
            self, projection, filters, limit,
        )?))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>, datafusion::error::DataFusionError> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

impl IcebergTableProvider {
    pub fn from_logical_plan(plan: &LogicalIcebergScan) -> RwResult<Self> {
        let source = plan.source_catalog().ok_or_else(|| {
            RwError::from(ErrorCode::InternalError(
                "Iceberg source catalog is missing".into(),
            ))
        })?;
        let config = ConnectorProperties::extract(source.with_properties.clone(), false)?;
        let ConnectorProperties::Iceberg(iceberg_properties) = config else {
            return Err(RwError::from(ErrorCode::ConnectorError(
                "Expected iceberg connector properties".into(),
            )));
        };
        let iceberg_properties = Arc::from(iceberg_properties);

        let arrow_fields: ArrowFields = plan
            .core
            .column_catalog
            .iter()
            .filter(|column| !column.is_hidden())
            .map(|column| {
                let column_desc = &column.column_desc;
                let field = IcebergArrowConvert
                    .to_arrow_field(&column_desc.name, &column_desc.data_type)?;
                Ok(field.with_nullable(column_desc.nullable))
            })
            .collect::<Result<_, ArrayError>>()?;
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

        Ok(Self {
            iceberg_properties,
            arrow_schema,
            snapshot_id: plan.snapshot_id,
            iceberg_scan_type: plan.iceberg_scan_type,
        })
    }
}
