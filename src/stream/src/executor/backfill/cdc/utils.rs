use std::sync::Arc;

use futures::pin_mut;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{DataChunk, StreamChunk};
use risingwave_common::catalog::{ColumnDesc, ColumnId, Schema};
use risingwave_common::row::{Row, RowExt};
use risingwave_common::types::{DataType, ScalarRefImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::parser::{
    DebeziumParser, EncodingProperties, JsonProperties, ProtocolProperties,
    SourceStreamChunkBuilder, SpecificParserConfig,
};
use risingwave_connector::source::{SourceColumnDesc, SourceContext};

use crate::executor::{BoxedMessageStream, Message, StreamExecutorError, StreamExecutorResult};

#[try_stream(ok = Message, error = StreamExecutorError)]
pub async fn transform_upstream(upstream: BoxedMessageStream, schema: &Schema) {
    let props = SpecificParserConfig {
        key_encoding_config: None,
        encoding_config: EncodingProperties::Json(JsonProperties {
            use_schema_registry: false,
        }),
        protocol_config: ProtocolProperties::Debezium,
    };
    let mut parser = DebeziumParser::new(
        props,
        get_rw_columns(schema),
        Arc::new(SourceContext::default()),
    )
    .await
    .map_err(StreamExecutorError::connector_error)?;

    pin_mut!(upstream);
    #[for_await]
    for msg in upstream {
        let mut msg = msg?;
        if let Message::Chunk(chunk) = &mut msg {
            let parsed_chunk = parse_debezium_chunk(&mut parser, chunk, schema).await?;
            let _ = std::mem::replace(chunk, parsed_chunk);
        }
        yield msg;
    }
}

async fn parse_debezium_chunk(
    parser: &mut DebeziumParser,
    chunk: &StreamChunk,
    schema: &Schema,
) -> StreamExecutorResult<StreamChunk> {
    // here we transform the input chunk in (payload varchar, _rw_offset varchar, _rw_table_name varchar) schema
    // to chunk with downstream table schema `info.schema` of MergeNode contains the schema of the
    // table job with `_rw_offset` in the end
    // see `gen_create_table_plan_for_cdc_source` for details
    let column_descs = get_rw_columns(schema);
    let mut builder = SourceStreamChunkBuilder::with_capacity(column_descs, chunk.capacity());

    // The schema of input chunk (payload varchar, _rw_offset varchar, _rw_table_name varchar, _row_id)
    // We should use the debezium parser to parse the first column,
    // then chain the parsed row with `_rw_offset` row to get a new row.
    let payloads = chunk.data_chunk().project(vec![0].as_slice());
    let offset_columns = chunk.data_chunk().project(vec![1].as_slice());

    // TODO: preserve the transaction semantics
    for payload in payloads.rows() {
        let ScalarRefImpl::Jsonb(jsonb_ref) = payload.datum_at(0).expect("payload must exist")
        else {
            unreachable!("payload must be jsonb");
        };

        parser
            .parse_inner(
                None,
                Some(jsonb_ref.to_string().as_bytes().to_vec()),
                builder.row_writer(),
            )
            .await
            .unwrap();
    }

    let parsed_chunk = builder.finish();
    let (data_chunk, ops) = parsed_chunk.into_parts();

    // concat the rows in the parsed chunk with the _rw_offset column, we should also retain the Op column
    let mut new_rows = Vec::with_capacity(chunk.capacity());
    for (data_row, offset_row) in data_chunk
        .rows_with_holes()
        .zip_eq_fast(offset_columns.rows_with_holes())
    {
        let combined = data_row.chain(offset_row);
        new_rows.push(combined);
    }

    let data_types = schema
        .fields
        .iter()
        .map(|field| field.data_type.clone())
        .chain(std::iter::once(DataType::Varchar)) // _rw_offset column
        .collect_vec();

    Ok(StreamChunk::from_parts(
        ops,
        DataChunk::from_rows(new_rows.as_slice(), data_types.as_slice()),
    ))
}

fn get_rw_columns(schema: &Schema) -> Vec<SourceColumnDesc> {
    schema
        .fields
        .iter()
        .map(|field| {
            let column_desc = ColumnDesc::named(
                field.name.clone(),
                ColumnId::placeholder(),
                field.data_type.clone(),
            );
            SourceColumnDesc::from(&column_desc)
        })
        .collect_vec()
}
