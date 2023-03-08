use anyhow::anyhow;
use risingwave_connector::sink::catalog::SinkCatalog;
use risingwave_connector::sink::{SinkConfig, SinkImpl};
use risingwave_pb::catalog::Sink as ProstSinkCatalog;

use crate::{MetaError, MetaResult};

pub async fn validate_sink(
    prost_sink_catalog: &ProstSinkCatalog,
    connector_rpc_endpoint: Option<String>,
) -> MetaResult<()> {
    let sink_catalog = SinkCatalog::from(prost_sink_catalog);
    let sink_config = SinkConfig::from_hashmap(sink_catalog.properties.clone())
        .map_err(|err| MetaError::from(anyhow!(err.to_string())))?;
    SinkImpl::validate(sink_config, sink_catalog, connector_rpc_endpoint)
        .await
        .map_err(|err| MetaError::from(anyhow!(err.to_string())))
}
