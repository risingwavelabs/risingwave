use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use rand::Rng;
use serde_derive::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::server_pb::server_client::ServerClient;
use crate::server_pb::{ReportTaxiActionRequest, GetTaxiAmountRequest};

#[derive(Serialize, Deserialize,Debug)]
pub struct TaxiFeature {
    pub(crate) VendorID: i32,
    pub(crate) lpep_pickup_datetime: String,
    pub(crate) lpep_dropoff_datetime: String,
    pub(crate) store_and_fwd_flag: String,
    pub(crate) RatecodeID: f64,
    pub(crate) PULocationID: i64,
    pub(crate) DOLocationID: i64,
    pub(crate) passenger_count: f64,
    pub(crate) trip_distance: f64,
    pub(crate) fare_amount: f64,
    pub(crate) extra: f64,
    pub(crate) mta_tax: f64,
    pub(crate) tip_amount: f64,
    pub(crate) tolls_amount: f64,
    pub(crate) ehail_fee: Option<f64>,
    pub(crate) improvement_surcharge: f64,
    pub(crate) total_amount: f64,
    pub(crate) payment_type: f64,
    pub(crate) trip_type: f64,
    pub(crate) congestion_surcharge: f64,
}

pub fn read_feature_for_csv(path: PathBuf) -> Result<Vec<TaxiFeature>, Box<dyn Error>>{
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut reader = csv::Reader::from_reader(reader);
    let mut records = vec![];
    for record in reader.deserialize() {
        let record:TaxiFeature = record.unwrap();
        // println!(
        //     "{:?}",record
        // );
        records.push(record);
    }
    Ok(records)
}

pub fn parse_taxi_metadata() -> (Vec<TaxiFeature>,Vec<TaxiFeature>){

    let mut offlines = read_feature_for_csv(Path::new("../").join("parquet_data.csv")).unwrap();

    let onlines = offlines.split_off(offlines.len() / 10 * 9);
    
    (offlines,onlines)
}

impl TaxiFeature {
    pub(crate) async fn mock_act<'a>(
        &'a self,
        client: &'a mut ServerClient<Channel>,
    ) -> Result<(), &str> {
        let response = client
            .report_taxi_action(tonic::Request::new(ReportTaxiActionRequest {
                vendor_id: self.VendorID,
                lpep_pickup_datetime: self.lpep_pickup_datetime.clone(),
                lpep_dropoff_datetime: self.lpep_dropoff_datetime.clone(),
                store_and_fwd_flag: self.store_and_fwd_flag.eq("N"),
                ratecode_id: self.RatecodeID,
                pu_location_id: self.PULocationID,
                do_location_id: self.DOLocationID,
                passenger_count: self.passenger_count,
                trip_distance: self.trip_distance,
                fare_amount: self.fare_amount,
                extra: self.extra,
                mta_tax: self.mta_tax,
                tip_amount: self.tip_amount,
                tolls_amount: self.tolls_amount,
                ehail_fee: self.ehail_fee.unwrap_or_else(||{0.0}),
                improvement_surcharge: self.improvement_surcharge,
                total_amount: self.total_amount,
                payment_type: self.payment_type,
                trip_type: self.trip_type,
                congestion_surcharge: self.congestion_surcharge,
            }))
            .await
        .unwrap();
        Ok(())
    }

    pub async fn mock_get_amount(&self, client: &mut ServerClient<Channel>) -> f64 {
        let response = client
            .get_taxi_amount(GetTaxiAmountRequest {
                do_location_id: self.DOLocationID,
            })
            .await
            .unwrap();

        let inner = response.into_inner();
        inner.fare_amount
    }
}