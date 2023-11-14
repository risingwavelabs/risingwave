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

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
use arrow_schema::Schema;
use cfg_or_panic::cfg_or_panic;
use futures_util::{stream, Stream, StreamExt, TryStreamExt};
use tonic::transport::Channel;

use crate::{Error, Result};

/// Client for external function service based on Arrow Flight.
#[derive(Debug)]
pub struct ArrowFlightUdfClient {
    client: FlightServiceClient<Channel>,
}

// TODO: support UDF in simulation
#[cfg_or_panic(not(madsim))]
impl ArrowFlightUdfClient {
    /// Connect to a UDF service.
    pub async fn connect(addr: &str) -> Result<Self> {
        let client = FlightServiceClient::connect(addr.to_string()).await?;
        Ok(Self { client })
    }

    /// Connect to a UDF service lazily (i.e. only when the first request is sent).
    pub fn connect_lazy(addr: &str) -> Result<Self> {
        let conn = tonic::transport::Endpoint::new(addr.to_string())?.connect_lazy();
        let client = FlightServiceClient::new(conn);
        Ok(Self { client })
    }

    /// Check if the function is available and the schema is match.
    pub async fn check(&self, id: &str, args: &Schema, returns: &Schema) -> Result<()> {
        let descriptor = FlightDescriptor::new_path(vec![id.into()]);

        let response = self.client.clone().get_flight_info(descriptor).await?;

        // check schema
        let info = response.into_inner();
        let input_num = info.total_records as usize;
        let full_schema = Schema::try_from(info)
            .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?;
        if input_num > full_schema.fields.len() {
            return Err(Error::service_error(format!(
                "function {:?} schema info not consistency: input_num: {}, total_fields: {}",
                id,
                input_num,
                full_schema.fields.len()
            )));
        }

        let (input_fields, return_fields) = full_schema.fields.split_at(input_num);
        let actual_input_types: Vec<_> = input_fields.iter().map(|f| f.data_type()).collect();
        let actual_result_types: Vec<_> = return_fields.iter().map(|f| f.data_type()).collect();
        let expect_input_types: Vec<_> = args.fields.iter().map(|f| f.data_type()).collect();
        let expect_result_types: Vec<_> = returns.fields.iter().map(|f| f.data_type()).collect();
        if !data_types_match(&expect_input_types, &actual_input_types) {
            return Err(Error::type_mismatch(format!(
                "function: {:?}, expect arguments: {:?}, actual: {:?}",
                id, expect_input_types, actual_input_types
            )));
        }
        if !data_types_match(&expect_result_types, &actual_result_types) {
            return Err(Error::type_mismatch(format!(
                "function: {:?}, expect return: {:?}, actual: {:?}",
                id, expect_result_types, actual_result_types
            )));
        }
        Ok(())
    }

    /// Call a function.
    pub async fn call(&self, id: &str, input: RecordBatch) -> Result<RecordBatch> {
        let mut output_stream = self.call_stream(id, stream::once(async { input })).await?;
        // TODO: support no output
        let head = output_stream
            .next()
            .await
            .ok_or_else(Error::no_returned)??;
        let mut remaining = vec![];
        while let Some(batch) = output_stream.next().await {
            remaining.push(batch?);
        }
        if remaining.is_empty() {
            Ok(head)
        } else {
            Ok(arrow_select::concat::concat_batches(
                &head.schema(),
                std::iter::once(&head).chain(remaining.iter()),
            )?)
        }
    }

    /// Call a function with streaming input and output.
    #[panic_return = "Result<stream::Empty<_>>"]
    pub async fn call_stream(
        &self,
        id: &str,
        inputs: impl Stream<Item = RecordBatch> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        let descriptor = FlightDescriptor::new_path(vec![id.into()]);
        let flight_data_stream = FlightDataEncoderBuilder::new()
            // XXX(wrj): unlimit the size of flight data to avoid splitting batch
            //           there's a bug in arrow-flight when splitting batch with list type array
            // FIXME: remove this when the bug is fixed in arrow-flight
            .with_max_flight_data_size(usize::MAX)
            .build(inputs.map(Ok))
            .map(move |res| FlightData {
                // TODO: fill descriptor only for the first message
                flight_descriptor: Some(descriptor.clone()),
                ..res.unwrap()
            });

        // call `do_exchange` on Flight server
        let response = self.client.clone().do_exchange(flight_data_stream).await?;

        // decode response
        let stream = response.into_inner();
        let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            // convert tonic::Status to FlightError
            stream.map_err(|e| e.into()),
        );
        Ok(record_batch_stream.map_err(|e| e.into()))
    }
}

/// Check if two list of data types match, ignoring field names.
fn data_types_match(a: &[&arrow_schema::DataType], b: &[&arrow_schema::DataType]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    #[allow(clippy::disallowed_methods)]
    a.iter().zip(b.iter()).all(|(a, b)| a.equals_datatype(b))
}
