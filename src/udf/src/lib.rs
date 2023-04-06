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
use futures_util::{stream, Stream, StreamExt, TryStreamExt};
use tonic::transport::Channel;

/// Client for external function service based on Arrow Flight.
#[derive(Debug)]
pub struct ArrowFlightUdfClient {
    client: FlightServiceClient<Channel>,
}

#[cfg(not(madsim))]
impl ArrowFlightUdfClient {
    /// Connect to a UDF service.
    pub async fn connect(addr: &str) -> Result<Self> {
        let client = FlightServiceClient::connect(addr.to_string()).await?;
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
        let (input_fields, return_fields) = full_schema.fields.split_at(input_num);
        let actual_input_types: Vec<_> = input_fields.iter().map(|f| f.data_type()).collect();
        let actual_result_types: Vec<_> = return_fields.iter().map(|f| f.data_type()).collect();
        let expect_input_types: Vec<_> = args.fields.iter().map(|f| f.data_type()).collect();
        let expect_result_types: Vec<_> = returns.fields.iter().map(|f| f.data_type()).collect();
        if !data_types_match(&expect_input_types, &actual_input_types) {
            return Err(Error::ArgumentMismatch {
                function_id: id.into(),
                expected: format!("{:?}", expect_input_types),
                actual: format!("{:?}", actual_input_types),
            });
        }
        if !data_types_match(&expect_result_types, &actual_result_types) {
            return Err(Error::ReturnTypeMismatch {
                function_id: id.into(),
                expected: format!("{:?}", expect_result_types),
                actual: format!("{:?}", actual_result_types),
            });
        }
        Ok(())
    }

    /// Call a function.
    pub async fn call(&self, id: &str, input: RecordBatch) -> Result<RecordBatch> {
        let mut output_stream = self.call_stream(id, stream::once(async { input })).await?;
        // TODO: support no output
        let head = output_stream.next().await.ok_or(Error::NoReturned)??;
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
    pub async fn call_stream(
        &self,
        id: &str,
        inputs: impl Stream<Item = RecordBatch> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        let descriptor = FlightDescriptor::new_path(vec![id.into()]);
        let flight_data_stream =
            FlightDataEncoderBuilder::new()
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

// TODO: support UDF in simulation
#[cfg(madsim)]
impl ArrowFlightUdfClient {
    /// Connect to a UDF service.
    pub async fn connect(_addr: &str) -> Result<Self> {
        panic!("UDF is not supported in simulation yet")
    }

    /// Check if the function is available.
    pub async fn check(&self, _id: &str, _args: &Schema, _returns: &Schema) -> Result<()> {
        panic!("UDF is not supported in simulation yet")
    }

    /// Call a function.
    pub async fn call(&self, _id: &str, _input: RecordBatch) -> Result<RecordBatch> {
        panic!("UDF is not supported in simulation yet")
    }

    /// Call a function with streaming input and output.
    pub async fn call_stream(
        &self,
        _id: &str,
        _inputs: impl Stream<Item = RecordBatch> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        panic!("UDF is not supported in simulation yet");
        Ok(stream::empty())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to connect to UDF service: {0}")]
    Connect(#[from] tonic::transport::Error),
    #[error("failed to check UDF: {0}")]
    Tonic(#[from] tonic::Status),
    #[error("failed to call UDF: {0}")]
    Flight(#[from] FlightError),
    #[error("argument mismatch: function {function_id:?}, expected {expected}, actual {actual}")]
    ArgumentMismatch {
        function_id: String,
        expected: String,
        actual: String,
    },
    #[error(
        "return type mismatch: function {function_id:?}, expected {expected}, actual {actual}"
    )]
    ReturnTypeMismatch {
        function_id: String,
        expected: String,
        actual: String,
    },
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("UDF service returned no data")]
    NoReturned,
}

/// Check if two list of data types match, ignoring field names.
fn data_types_match(a: &[&arrow_schema::DataType], b: &[&arrow_schema::DataType]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    #[allow(clippy::disallowed_methods)]
    a.iter().zip(b.iter()).all(|(a, b)| a.equals_datatype(b))
}
