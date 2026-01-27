use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::server_pb::server_client::ServerClient;
use crate::server_pb::{GetTaxiAmountRequest, ReportTaxiActionRequest};

#[derive(Serialize, Deserialize, Debug)]
pub struct TaxiFeature {
    #[serde(rename = "vendorID")]
    pub(crate) vendor_id: i32,
    #[serde(rename = "lpepPickupDatetime")]
    pub(crate) lpep_pickup_datetime: String,
    #[serde(rename = "lpepDropoffDatetime")]
    pub(crate) lpep_dropoff_datetime: String,
    #[serde(rename = "storeAndFwdFlag")]
    pub(crate) store_and_fwd_flag: String,
    #[serde(rename = "rateCodeID")]
    pub(crate) ratecode_id: f64,
    #[serde(rename = "puLocationId")]
    pub(crate) pulocation_id: i64,
    #[serde(rename = "doLocationId")]
    pub(crate) dolocation_id: i64,
    #[serde(rename = "pickupLongitude")]
    pub(crate) pickup_longitude: Option<f64>,
    #[serde(rename = "pickupLatitude")]
    pub(crate) pickup_latitude: Option<f64>,
    #[serde(rename = "dropofflongitude")]
    pub(crate) dropoff_longitude: Option<f64>,
    #[serde(rename = "dropoffLatitude")]
    pub(crate) dropoff_latitude: Option<f64>,
    #[serde(rename = "passengerCount")]
    pub(crate) passenger_count: f64,
    #[serde(rename = "tripDistance")]
    pub(crate) trip_distance: f64,
    #[serde(rename = "fareAmount")]
    pub(crate) fare_amount: f64,
    #[serde(rename = "extra")]
    pub(crate) extra: f64,
    #[serde(rename = "mtaTax")]
    pub(crate) mta_tax: f64,
    #[serde(rename = "tipAmount")]
    pub(crate) tip_amount: f64,
    #[serde(rename = "tollsAmount")]
    pub(crate) tolls_amount: f64,
    #[serde(rename = "ehailFee")]
    pub(crate) ehail_fee: Option<f64>,
    #[serde(rename = "improvementSurcharge")]
    pub(crate) improvement_surcharge: f64,
    #[serde(rename = "totalAmount")]
    pub(crate) total_amount: f64,
    #[serde(rename = "paymentType")]
    pub(crate) payment_type: f64,
    #[serde(rename = "tripType")]
    pub(crate) trip_type: f64,
}

pub fn read_feature_for_csv(path: PathBuf) -> Result<Vec<TaxiFeature>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut reader = csv::Reader::from_reader(reader);
    let mut records = vec![];
    for record in reader.deserialize() {
        let record: TaxiFeature = record.unwrap();
        records.push(record);
    }
    Ok(records)
}

pub fn parse_taxi_metadata() -> (Vec<TaxiFeature>, Vec<TaxiFeature>) {
    let path = std::env::var("WORK_DIR").unwrap_or("/opt/feature-store/".to_string());
    let mut offlines = read_feature_for_csv(Path::new(&*path).join("parquet_data.csv")).unwrap();

    let onlines = offlines.split_off(offlines.len() / 10 * 9);

    (offlines, onlines)
}

impl TaxiFeature {
    pub(crate) async fn mock_act<'a>(
        &'a self,
        client: &'a mut ServerClient<Channel>,
    ) -> Result<(), &str> {
        let _ = client
            .report_taxi_action(tonic::Request::new(ReportTaxiActionRequest {
                vendor_id: self.vendor_id,
                lpep_pickup_datetime: self.lpep_pickup_datetime.clone(),
                lpep_dropoff_datetime: self.lpep_dropoff_datetime.clone(),
                store_and_fwd_flag: self.store_and_fwd_flag.eq("N"),
                ratecode_id: self.ratecode_id,
                pu_location_id: self.pulocation_id,
                do_location_id: self.dolocation_id,
                passenger_count: self.passenger_count,
                trip_distance: self.trip_distance,
                fare_amount: self.fare_amount,
                extra: self.extra,
                mta_tax: self.mta_tax,
                tip_amount: self.tip_amount,
                tolls_amount: self.tolls_amount,
                ehail_fee: self.ehail_fee.unwrap_or(0.0),
                improvement_surcharge: self.improvement_surcharge,
                total_amount: self.total_amount,
                payment_type: self.payment_type,
                trip_type: self.trip_type,
                congestion_surcharge: 0.0,
            }))
            .await
            .unwrap();
        Ok(())
    }

    pub async fn mock_get_amount(&self, client: &mut ServerClient<Channel>) -> f64 {
        let response = client
            .get_taxi_amount(GetTaxiAmountRequest {
                do_location_id: self.dolocation_id,
                pu_location_id: self.pulocation_id,
            })
            .await
            .unwrap();

        let inner = response.into_inner();
        inner.fare_amount
    }
}
