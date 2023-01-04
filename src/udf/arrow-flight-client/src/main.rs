use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor};
use arrow_schema::{DataType, Field, Schema};
use futures_util::{stream, StreamExt, TryStreamExt};

#[tokio::main]
async fn main() {
    let addr = "http://localhost:8815";
    let mut client = FlightServiceClient::connect(addr).await.unwrap();

    // build `RecordBatch` to send (equivalent to our `DataChunk`)
    let array1 = Int32Array::from_iter(vec![1, 6, 10]);
    let array2 = Int32Array::from_iter(vec![3, 4, 15]);
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array1), Arc::new(array2)]).unwrap();

    // build `FlightData` stream
    let input_stream = stream::iter(vec![Ok(batch)]);
    let flight_data_stream = FlightDataEncoderBuilder::new()
        .build(input_stream)
        .map(|res| FlightData {
            flight_descriptor: Some(FlightDescriptor::new_path(vec!["gcd".to_string()])),
            ..res.unwrap()
        });

    // call `do_exchange` on Flight server
    let response = client.do_exchange(flight_data_stream).await.unwrap();

    // read response
    let stream = response.into_inner();
    let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
        // convert tonic::Status to FlightError
        stream.map_err(|e| e.into()),
    );
    while let Some(batch) = record_batch_stream.next().await {
        dbg!(batch.unwrap());
    }
}
