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

    /// Check if the function is available and return the function ID.
    pub async fn check(&self, name: &str, args: &Schema, returns: &Schema) -> Result<FunctionId> {
        // path = name/[args,]*
        let mut path = name.to_string() + "/";
        for (i, arg) in args.fields.iter().enumerate() {
            if i != 0 {
                path += ",";
            }
            path += &arg.data_type().to_string().to_lowercase();
        }
        let descriptor = FlightDescriptor::new_path(vec![path.clone()]);

        let response = self.client.clone().get_flight_info(descriptor).await?;

        let info = response.into_inner();
        let schema = Schema::try_from(info)
            .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?;
        let expect_types: Vec<_> = returns.fields.iter().map(|f| f.data_type()).collect();
        let actual_types: Vec<_> = schema.fields.iter().map(|f| f.data_type()).collect();
        if expect_types != actual_types {
            return Err(Error::SchemaMismatch {
                function_name: name.into(),
                expected: format!("{:?}", expect_types),
                actual: format!("{:?}", actual_types),
            });
        }
        Ok(FunctionId(vec![path]))
    }

    /// Call a function.
    pub async fn call(&self, id: &FunctionId, input: RecordBatch) -> Result<RecordBatch> {
        let mut output_stream = self.call_stream(id, stream::once(async { input })).await?;
        output_stream.next().await.ok_or(Error::NoReturned)?
    }

    /// Call a function with streaming input and output.
    pub async fn call_stream(
        &self,
        id: &FunctionId,
        inputs: impl Stream<Item = RecordBatch> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        let descriptor = FlightDescriptor::new_path(id.0.clone());
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

    /// Check if the function is available and return the function ID.
    pub async fn check(
        &self,
        _name: &str,
        _args: &Schema,
        _returns: &Schema,
    ) -> Result<FunctionId> {
        panic!("UDF is not supported in simulation yet")
    }

    /// Call a function.
    pub async fn call(&self, _id: &FunctionId, _input: RecordBatch) -> Result<RecordBatch> {
        panic!("UDF is not supported in simulation yet")
    }

    /// Call a function with streaming input and output.
    pub async fn call_stream(
        &self,
        _id: &FunctionId,
        _inputs: impl Stream<Item = RecordBatch> + Send + 'static,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'static> {
        panic!("UDF is not supported in simulation yet");
        Ok(stream::empty())
    }
}

/// An opaque ID for a function.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FunctionId(Vec<String>);

pub type Result<T> = std::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("failed to connect to UDF service: {0}")]
    Connect(#[from] tonic::transport::Error),
    #[error("failed to check UDF: {0}")]
    Tonic(#[from] tonic::Status),
    #[error("failed to call UDF: {0}")]
    Flight(#[from] FlightError),
    #[error("schema mismatch: function {function_name:?}, expected return types {expected}, actual {actual}")]
    SchemaMismatch {
        function_name: String,
        expected: String,
        actual: String,
    },
    #[error("UDF service returned no data")]
    NoReturned,
    #[error("UDF service returned a batch with no column")]
    NoColumn,
}
