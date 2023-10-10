// Copyright 2023 RisingWave Labs
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

use std::assert_matches::assert_matches;
use std::io::{Error, ErrorKind};

use anyhow::anyhow;
use fixedbitset::FixedBitSet;
use itertools::Itertools;
use pretty_xmlish::{Pretty, XmlNode};
use risingwave_common::catalog::{ColumnCatalog, Field};
use risingwave_common::constants::log_store::{
    EPOCH_COLUMN_INDEX, KV_LOG_STORE_PREDEFINED_COLUMNS, SEQ_ID_COLUMN_INDEX,
};
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::session_config::sink_decouple::SinkDecouple;
use risingwave_common::util::sort_util::OrderType;
use risingwave_connector::match_sink_name_str;
use risingwave_connector::sink::catalog::desc::SinkDesc;
use risingwave_connector::sink::catalog::{SinkFormat, SinkFormatDesc, SinkId, SinkType};
use risingwave_connector::sink::{
    SinkError, CONNECTOR_TYPE_KEY, SINK_TYPE_APPEND_ONLY, SINK_TYPE_DEBEZIUM, SINK_TYPE_OPTION,
    SINK_TYPE_UPSERT, SINK_USER_FORCE_APPEND_ONLY_OPTION,
};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use tracing::info;

use super::derive::{derive_columns, derive_pk};
use super::utils::{childless_record, Distill, IndicesDisplay, TableCatalogBuilder};
use super::{ExprRewritable, PlanBase, PlanRef, StreamNode};
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::optimizer::property::{Distribution, Order, RequiredDist};
use crate::stream_fragmenter::BuildFragmentGraphState;
use crate::{TableCatalog, WithOptions};

const DOWNSTREAM_PK_KEY: &str = "primary_key";

/// [`StreamSink`] represents a table/connector sink at the very end of the graph.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamSink {
    pub base: PlanBase,
    input: PlanRef,
    sink_desc: SinkDesc,
}

impl StreamSink {
    #[must_use]
    pub fn new(input: PlanRef, sink_desc: SinkDesc) -> Self {
        let base = PlanBase::derive_stream_plan_base(&input);
        Self {
            base,
            input,
            sink_desc,
        }
    }

    pub fn sink_desc(&self) -> &SinkDesc {
        &self.sink_desc
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        input: PlanRef,
        name: String,
        db_name: String,
        sink_from_table_name: String,
        user_distributed_by: RequiredDist,
        user_order_by: Order,
        user_cols: FixedBitSet,
        out_names: Vec<String>,
        definition: String,
        properties: WithOptions,
        format_desc: Option<SinkFormatDesc>,
    ) -> Result<Self> {
        let columns = derive_columns(input.schema(), out_names, &user_cols)?;
        let (input, sink) = Self::derive_sink_desc(
            input,
            user_distributed_by,
            name,
            db_name,
            sink_from_table_name,
            user_order_by,
            columns,
            definition,
            properties,
            format_desc,
        )?;

        // check and ensure that the sink connector is specified and supported
        match sink.properties.get(CONNECTOR_TYPE_KEY) {
            Some(connector) => match_sink_name_str!(
                connector.to_lowercase().as_str(),
                SinkType,
                Ok(()),
                |other| Err(SinkError::Config(anyhow!(
                    "unsupported sink type {}",
                    other
                )))
            )?,
            None => {
                return Err(
                    SinkError::Config(anyhow!("connector not specified when create sink")).into(),
                );
            }
        }

        Ok(Self::new(input, sink))
    }

    fn derive_sink_desc(
        input: PlanRef,
        user_distributed_by: RequiredDist,
        name: String,
        db_name: String,
        sink_from_name: String,
        user_order_by: Order,
        columns: Vec<ColumnCatalog>,
        definition: String,
        properties: WithOptions,
        format_desc: Option<SinkFormatDesc>,
    ) -> Result<(PlanRef, SinkDesc)> {
        let sink_type =
            Self::derive_sink_type(input.append_only(), &properties, format_desc.as_ref())?;
        let (pk, _) = derive_pk(input.clone(), user_order_by, &columns);
        let downstream_pk = Self::parse_downstream_pk(&columns, properties.get(DOWNSTREAM_PK_KEY))?;

        let required_dist = match input.distribution() {
            Distribution::Single => RequiredDist::single(),
            _ => {
                match properties.get("connector") {
                    Some(s) if s == "deltalake" => {
                        // iceberg with multiple parallelism will fail easily with concurrent commit
                        // on metadata
                        // TODO: reset iceberg sink to have multiple parallelism
                        info!("setting iceberg sink parallelism to singleton");
                        RequiredDist::single()
                    }
                    Some(s) if s == "jdbc" && sink_type == SinkType::Upsert => {
                        if sink_type == SinkType::Upsert && downstream_pk.is_empty() {
                            return Err(ErrorCode::SinkError(Box::new(Error::new(
                                ErrorKind::InvalidInput,
                                format!(
                                    "Primary key must be defined for upsert JDBC sink. Please specify the \"{key}='pk1,pk2,...'\" in WITH options.",
                                    key = DOWNSTREAM_PK_KEY
                                ),
                            )))
                                .into());
                        }
                        // for upsert jdbc sink we align distribution to downstream to avoid
                        // lock contentions
                        RequiredDist::hash_shard(downstream_pk.as_slice())
                    }
                    _ => {
                        assert_matches!(user_distributed_by, RequiredDist::Any);
                        RequiredDist::shard_by_key(input.schema().len(), input.expect_stream_key())
                    }
                }
            }
        };
        let input = required_dist.enforce_if_not_satisfies(input, &Order::any())?;
        let distribution_key = input.distribution().dist_column_indices().to_vec();
        let sink_desc = SinkDesc {
            id: SinkId::placeholder(),
            name,
            db_name,
            sink_from_name,
            definition,
            columns,
            plan_pk: pk,
            downstream_pk,
            distribution_key,
            properties: properties.into_inner(),
            sink_type,
            format_desc,
        };
        Ok((input, sink_desc))
    }

    fn is_user_defined_append_only(properties: &WithOptions) -> Result<bool> {
        if let Some(sink_type) = properties.get(SINK_TYPE_OPTION) {
            if sink_type != SINK_TYPE_APPEND_ONLY
                && sink_type != SINK_TYPE_DEBEZIUM
                && sink_type != SINK_TYPE_UPSERT
            {
                return Err(ErrorCode::SinkError(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    format!(
                        "`{}` must be {}, {}, or {}",
                        SINK_TYPE_OPTION,
                        SINK_TYPE_APPEND_ONLY,
                        SINK_TYPE_DEBEZIUM,
                        SINK_TYPE_UPSERT
                    ),
                )))
                .into());
            }
        }
        Ok(properties.value_eq_ignore_case(SINK_TYPE_OPTION, SINK_TYPE_APPEND_ONLY))
    }

    fn is_user_force_append_only(properties: &WithOptions) -> Result<bool> {
        if properties.contains_key(SINK_USER_FORCE_APPEND_ONLY_OPTION)
            && !properties.value_eq_ignore_case(SINK_USER_FORCE_APPEND_ONLY_OPTION, "true")
            && !properties.value_eq_ignore_case(SINK_USER_FORCE_APPEND_ONLY_OPTION, "false")
        {
            return Err(ErrorCode::SinkError(Box::new(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "`{}` must be true or false",
                    SINK_USER_FORCE_APPEND_ONLY_OPTION
                ),
            )))
            .into());
        }
        Ok(properties.value_eq_ignore_case(SINK_USER_FORCE_APPEND_ONLY_OPTION, "true"))
    }

    fn derive_sink_type(
        input_append_only: bool,
        properties: &WithOptions,
        format_desc: Option<&SinkFormatDesc>,
    ) -> Result<SinkType> {
        let frontend_derived_append_only = input_append_only;
        let (user_defined_append_only, user_force_append_only) = match format_desc {
            Some(f) => (
                f.format == SinkFormat::AppendOnly,
                Self::is_user_force_append_only(&WithOptions::from_inner(f.options.clone()))?,
            ),
            None => (
                Self::is_user_defined_append_only(properties)?,
                Self::is_user_force_append_only(properties)?,
            ),
        };

        match (
            frontend_derived_append_only,
            user_defined_append_only,
            user_force_append_only,
        ) {
            (true, true, _) => Ok(SinkType::AppendOnly),
            (false, true, true) => Ok(SinkType::ForceAppendOnly),
            (_, false, false) => Ok(SinkType::Upsert),
            (false, true, false) => {
                Err(ErrorCode::SinkError(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                        "The sink cannot be append-only. Please add \"force_append_only='true'\" in options to force the sink to be append-only. Notice that this will cause the sink executor to drop any UPDATE or DELETE message.",
                )))
                .into())
            }
            (_, false, true) => {
                Err(ErrorCode::SinkError(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    "Cannot force the sink to be append-only without \"FORMAT PLAIN\" or \"type='append-only'\".",
                )))
                .into())
            }
        }
    }

    /// Extract user-defined downstream pk columns from with options. Return the indices of the pk
    /// columns.
    ///
    /// The format of `downstream_pk_str` should be 'col1,col2,...' (delimited by `,`) in order to
    /// get parsed.
    fn parse_downstream_pk(
        columns: &[ColumnCatalog],
        downstream_pk_str: Option<&String>,
    ) -> Result<Vec<usize>> {
        match downstream_pk_str {
            Some(downstream_pk_str) => {
                // If the user defines the downstream primary key, we find out their indices.
                let downstream_pk = downstream_pk_str.split(',').collect_vec();
                let mut downstream_pk_indices = Vec::with_capacity(downstream_pk.len());
                for key in downstream_pk {
                    let trimmed_key = key.trim();
                    if trimmed_key.is_empty() {
                        continue;
                    }
                    match columns
                        .iter()
                        .position(|col| col.column_desc.name == trimmed_key)
                    {
                        Some(index) => downstream_pk_indices.push(index),
                        None => {
                            return Err(ErrorCode::SinkError(Box::new(Error::new(
                                ErrorKind::InvalidInput,
                                format!("Sink primary key column not found: {}. Please use ',' as the delimiter for different primary key columns.", trimmed_key),
                            )))
                            .into());
                        }
                    }
                }
                Ok(downstream_pk_indices)
            }
            None => {
                // The user doesn't define the downstream primary key and we simply return an empty
                // vector.
                Ok(Vec::new())
            }
        }
    }

    /// The table schema is: | epoch | seq id | row op | sink columns |
    /// Pk is: | epoch | seq id |
    fn infer_kv_log_store_table_catalog(&self) -> TableCatalog {
        let mut table_catalog_builder =
            TableCatalogBuilder::new(self.input.ctx().with_options().internal_table_subset());

        let mut value_indices = Vec::with_capacity(
            KV_LOG_STORE_PREDEFINED_COLUMNS.len() + self.sink_desc.columns.len(),
        );

        for (name, data_type) in KV_LOG_STORE_PREDEFINED_COLUMNS {
            let indice = table_catalog_builder.add_column(&Field::with_name(data_type, name));
            value_indices.push(indice);
        }

        // The table's pk is composed of `epoch` and `seq_id`.
        table_catalog_builder.add_order_column(EPOCH_COLUMN_INDEX, OrderType::ascending());
        table_catalog_builder
            .add_order_column(SEQ_ID_COLUMN_INDEX, OrderType::ascending_nulls_last());
        let read_prefix_len_hint = table_catalog_builder.get_current_pk_len();

        let payload_indices = table_catalog_builder.extend_columns(&self.sink_desc().columns);

        value_indices.extend(payload_indices);
        table_catalog_builder.set_value_indices(value_indices);

        // Modify distribution key indices based on the pre-defined columns.
        let dist_key = self
            .input
            .distribution()
            .dist_column_indices()
            .iter()
            .map(|idx| idx + KV_LOG_STORE_PREDEFINED_COLUMNS.len())
            .collect_vec();

        table_catalog_builder.build(dist_key, read_prefix_len_hint)
    }
}

impl PlanTreeNodeUnary for StreamSink {
    fn input(&self) -> PlanRef {
        self.input.clone()
    }

    fn clone_with_input(&self, input: PlanRef) -> Self {
        Self::new(input, self.sink_desc.clone())
        // TODO(nanderstabel): Add assertions (assert_eq!)
    }
}

impl_plan_tree_node_for_unary! { StreamSink }

impl Distill for StreamSink {
    fn distill<'a>(&self) -> XmlNode<'a> {
        let sink_type = if self.sink_desc.sink_type.is_append_only() {
            "append-only"
        } else {
            "upsert"
        };
        let column_names = self
            .sink_desc
            .columns
            .iter()
            .map(|col| col.name_with_hidden().to_string())
            .map(Pretty::from)
            .collect();
        let column_names = Pretty::Array(column_names);
        let mut vec = Vec::with_capacity(3);
        vec.push(("type", Pretty::from(sink_type)));
        vec.push(("columns", column_names));
        if self.sink_desc.sink_type.is_upsert() {
            let pk = IndicesDisplay {
                indices: &self
                    .sink_desc
                    .plan_pk
                    .iter()
                    .map(|k| k.column_index)
                    .collect_vec(),
                schema: &self.base.schema,
            };
            vec.push(("pk", pk.distill()));
        }
        childless_record("StreamSink", vec)
    }
}

impl StreamNode for StreamSink {
    fn to_stream_prost_body(&self, state: &mut BuildFragmentGraphState) -> PbNodeBody {
        use risingwave_pb::stream_plan::*;

        // We need to create a table for sink with a kv log store.
        let table = self
            .infer_kv_log_store_table_catalog()
            .with_id(state.gen_table_id_wrapped());

        PbNodeBody::Sink(SinkNode {
            sink_desc: Some(self.sink_desc.to_proto()),
            table: Some(table.to_internal_table_prost()),
            log_store_type: match self.base.ctx.session_ctx().config().get_sink_decouple() {
                SinkDecouple::Default => {
                    let enable_sink_decouple =
                        match_sink_name_str!(
                        self.sink_desc.properties.get(CONNECTOR_TYPE_KEY).expect(
                            "have checked connector is contained when create the `StreamSink`"
                        ).to_lowercase().as_str(),
                        SinkTypeName,
                        SinkTypeName::default_sink_decouple(&self.sink_desc),
                        |_unsupported| unreachable!(
                            "have checked connector is supported when create the `StreamSink`"
                        )
                    );
                    if enable_sink_decouple {
                        SinkLogStoreType::KvLogStore as i32
                    } else {
                        SinkLogStoreType::InMemoryLogStore as i32
                    }
                }
                SinkDecouple::Enable => SinkLogStoreType::KvLogStore as i32,
                SinkDecouple::Disable => SinkLogStoreType::InMemoryLogStore as i32,
            },
        })
    }
}

impl ExprRewritable for StreamSink {}
