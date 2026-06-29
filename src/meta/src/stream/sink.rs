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

use anyhow::Context;
use risingwave_common::types::DataType;
use risingwave_connector::dispatch_sink;
use risingwave_connector::sink::catalog::{SinkCatalog, SinkEncode};
use risingwave_connector::sink::doris::DORIS_SINK;
use risingwave_connector::sink::elasticsearch_opensearch::elasticsearch::ES_SINK;
use risingwave_connector::sink::elasticsearch_opensearch::opensearch::OPENSEARCH_SINK;
use risingwave_connector::sink::file_sink::s3::SNOWFLAKE_SINK;
use risingwave_connector::sink::nats::NATS_SINK;
use risingwave_connector::sink::snowflake_redshift::redshift::REDSHIFT_SINK;
use risingwave_connector::sink::snowflake_redshift::snowflake::SNOWFLAKE_SINK_V2;
use risingwave_connector::sink::starrocks::STARROCKS_SINK;
use risingwave_connector::sink::{CONNECTOR_TYPE_KEY, Sink, SinkParam, build_sink};
use risingwave_pb::catalog::PbSink;

use crate::{MetaError, MetaResult};

#[await_tree::instrument(boxed)]
pub async fn validate_sink(prost_sink_catalog: &PbSink) -> MetaResult<()> {
    let sink_catalog = SinkCatalog::from(prost_sink_catalog);
    reject_variant_for_json_sink(&sink_catalog)?;
    let param = SinkParam::try_from_sink_catalog(sink_catalog)?;

    let sink = build_sink(param)?;

    dispatch_sink!(
        sink,
        sink,
        Ok(sink.validate().await.context("failed to validate sink")?)
    )
}

fn reject_variant_for_json_sink(sink_catalog: &SinkCatalog) -> MetaResult<()> {
    if !sink_uses_json_encoder(sink_catalog) {
        return Ok(());
    }

    if let Some(column) = sink_catalog
        .full_columns()
        .iter()
        .find(|column| matches!(column.data_type(), DataType::Variant))
    {
        return Err(MetaError::invalid_parameter(format!(
            "JSON sink encoders do not support VARIANT columns yet: column `{}`",
            column.name_with_hidden()
        )));
    }

    Ok(())
}

fn sink_uses_json_encoder(sink_catalog: &SinkCatalog) -> bool {
    if matches!(
        sink_catalog.format_desc.as_ref().map(|desc| &desc.encode),
        Some(SinkEncode::Json)
    ) {
        return true;
    }

    let Some(connector) = sink_catalog.properties.get(CONNECTOR_TYPE_KEY) else {
        return false;
    };

    [
        DORIS_SINK,
        ES_SINK,
        NATS_SINK,
        OPENSEARCH_SINK,
        REDSHIFT_SINK,
        SNOWFLAKE_SINK,
        SNOWFLAKE_SINK_V2,
        STARROCKS_SINK,
    ]
    .into_iter()
    .any(|json_sink| connector.eq_ignore_ascii_case(json_sink))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use risingwave_common::catalog::{ColumnCatalog, ColumnDesc, ColumnId};
    use risingwave_common::types::DataType;
    use risingwave_connector::sink::doris::DORIS_SINK;
    use risingwave_pb::catalog::{
        PbSink, SinkFormatDesc as PbSinkFormatDesc, SinkType as PbSinkType,
    };
    use risingwave_pb::plan_common::{EncodeType, FormatType};

    use super::*;

    fn make_sink(
        connector: &str,
        format_desc: Option<PbSinkFormatDesc>,
        data_type: DataType,
    ) -> SinkCatalog {
        SinkCatalog::from(PbSink {
            columns: vec![
                ColumnCatalog::visible(ColumnDesc::named("payload", ColumnId::new(0), data_type))
                    .to_protobuf(),
            ],
            format_desc,
            properties: BTreeMap::from([(CONNECTOR_TYPE_KEY.to_owned(), connector.to_owned())]),
            sink_type: PbSinkType::AppendOnly as i32,
            ..Default::default()
        })
    }

    fn format_desc(encode: EncodeType) -> PbSinkFormatDesc {
        PbSinkFormatDesc {
            format: FormatType::Plain as i32,
            encode: encode as i32,
            ..Default::default()
        }
    }

    #[test]
    fn test_reject_variant_for_format_json_sink() {
        let sink = make_sink(
            "kafka",
            Some(format_desc(EncodeType::Json)),
            DataType::Variant,
        );

        let err = reject_variant_for_json_sink(&sink).unwrap_err();

        assert!(
            err.to_string().contains(
                "JSON sink encoders do not support VARIANT columns yet: column `payload`"
            ),
            "{err:?}"
        );
    }

    #[test]
    fn test_reject_variant_for_connector_json_sink() {
        let sink = make_sink(DORIS_SINK, None, DataType::Variant);

        let err = reject_variant_for_json_sink(&sink).unwrap_err();

        assert!(
            err.to_string().contains(
                "JSON sink encoders do not support VARIANT columns yet: column `payload`"
            ),
            "{err:?}"
        );
    }

    #[test]
    fn test_allow_variant_for_non_json_sink() {
        let sink = make_sink(
            "kafka",
            Some(format_desc(EncodeType::Avro)),
            DataType::Variant,
        );

        reject_variant_for_json_sink(&sink).unwrap();
    }

    #[test]
    fn test_allow_json_sink_without_variant() {
        let sink = make_sink(
            "kafka",
            Some(format_desc(EncodeType::Json)),
            DataType::Jsonb,
        );

        reject_variant_for_json_sink(&sink).unwrap();
    }
}
