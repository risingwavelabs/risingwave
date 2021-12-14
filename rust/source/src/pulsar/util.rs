use pulsar::consumer::InitialPosition;
use pulsar::reader::Reader;
use pulsar::{ConsumerOptions, Pulsar, TokioExecutor};
use risingwave_common::array::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;

pub(crate) async fn build_pulsar_reader(
    address: &str,
    topic: &str,
) -> Result<Reader<Vec<u8>, TokioExecutor>> {
    let pulsar: Pulsar<_> = Pulsar::builder(address, TokioExecutor)
        .build()
        .await
        .map_err(|e| RwError::from(InternalError(e.to_string())))?;

    let reader: Reader<Vec<u8>, _> = pulsar
        .reader()
        .with_topic(topic)
        .with_batch_size(DEFAULT_CHUNK_BUFFER_SIZE as u32)
        .with_options(ConsumerOptions {
            initial_position: InitialPosition::Earliest,
            ..Default::default()
        })
        .into_reader()
        .await
        .map_err(|e| RwError::from(InternalError(e.to_string())))?;

    Ok(reader)
}
