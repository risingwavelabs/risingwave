use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};

use crate::kafka::KafkaSink;
use crate::server_pb::server_server::Server;
use crate::server_pb::{
    GetFeatureRequest, GetFeatureResponse, GetTaxiAmountRequest, GetTaxiAmountResponse,
    ReportActionRequest, ReportActionResponse, ReportTaxiActionRequest, ReportTaxiActionResponse,
    StartTrainingRequest, StartTrainingResponse,
};

pub struct FeatureStoreServer {
    pub(crate) kafka: KafkaSink,
}

#[tonic::async_trait]
impl Server for FeatureStoreServer {
    async fn get_feature(
        &self,
        request: Request<GetFeatureRequest>,
    ) -> Result<Response<GetFeatureResponse>, Status> {
        let userid = request.into_inner().userid;
        println!("MFA: get_feature: userid={}", userid);
        let (count, sum) = self.get_mfa_feature_from_rw(userid.clone()).await.unwrap();
        Ok(Response::new(GetFeatureResponse {
            count: count,
            sum: sum,
        }))
    }

    async fn report_action(
        &self,
        request: Request<ReportActionRequest>,
    ) -> Result<Response<ReportActionResponse>, Status> {
        let message = request.into_inner();
        self.mock_report_action(&message).await
    }

    async fn report_taxi_action(
        &self,
        request: tonic::Request<ReportTaxiActionRequest>,
    ) -> Result<tonic::Response<ReportTaxiActionResponse>, tonic::Status> {
        let message = request.into_inner();
        self.mock_report_taxi_action(&message).await
    }

    async fn start_training(
        &self,
        _request: tonic::Request<StartTrainingRequest>,
    ) -> Result<tonic::Response<StartTrainingResponse>, tonic::Status> {
        self.do_training().await
    }

    async fn get_taxi_amount(
        &self,
        request: tonic::Request<GetTaxiAmountRequest>,
    ) -> Result<tonic::Response<GetTaxiAmountResponse>, tonic::Status> {
        let do_location_id = request.into_inner();
        let fare_amount = self.get_taxi_amount(do_location_id.do_location_id.clone(),do_location_id.pu_location_id.clone()).await.unwrap();
        Ok(Response::new(GetTaxiAmountResponse {
            fare_amount: fare_amount as f64,
        }))
    }
}

impl FeatureStoreServer {
    async fn mock_report_action(
        &self,
        message: &ReportActionRequest,
    ) -> Result<Response<ReportActionResponse>, Status> {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH);
        match duration {
            Ok(dur) => {
                let timestamp = dur.as_micros();
                let json = Self::create_sink_mfa_json(message, timestamp as u64);
                println!("timestamp: {}, payload: {}", timestamp, json.clone());
                self.kafka.send("0".to_string(), json).await;
                Ok(Response::new(ReportActionResponse {
                    timestamp: timestamp as u64,
                }))
            }
            Err(_e) => Err(Status::unknown("Failed to generate timestamp".to_string())),
        }
    }

    pub(crate) fn create_sink_mfa_json(message: &ReportActionRequest, timestamp: u64) -> String {
        format!(
            "{{\"userid\": {:?}, \"eventype\": {:?}, \"changenum\": {:?}, \"timestamp\": {:?}}}",
            message.userid, message.eventtype, message.changenum, timestamp
        )
        .to_string()
    }

    async fn mock_report_taxi_action(
        &self,
        message: &ReportTaxiActionRequest,
    ) -> Result<Response<ReportTaxiActionResponse>, Status> {
        let json = Self::create_sink_taxi_json(message);
        self.kafka.send("0".to_string(), json).await;
        Ok(Response::new(ReportTaxiActionResponse {}))
    }

    pub(crate) fn create_sink_taxi_json(message: &ReportTaxiActionRequest) -> String {
        format!(
            "{{\"vendor_id\": {:?}, \"lpep_pickup_datetime\": {:?}, \"lpep_dropoff_datetime\": {:?}, \"store_and_fwd_flag\": {:?},
            \"ratecode_id\": {:?}, \"pu_location_id\": {:?}, \"do_location_id\": {:?}, \"passenger_count\": {:?},
            \"trip_distance\": {:?}, \"fare_amount\": {:?}, \"extra\": {:?}, \"mta_tax\": {:?},
            \"tip_amount\": {:?}, \"tolls_amount\": {:?}, \"ehail_fee\": {:?}, \"improvement_surcharge\": {:?},
            \"total_amount\": {:?}, \"payment_type\": {:?}, \"trip_type\": {:?}, \"congestion_surcharge\": {:?}}}",
            message.vendor_id,message.lpep_pickup_datetime,message.lpep_dropoff_datetime,message.store_and_fwd_flag,
            message.ratecode_id,message.pu_location_id,message.do_location_id,message.passenger_count,
            message.trip_distance,message.fare_amount,message.extra,message.mta_tax,
            message.tip_amount,message.tolls_amount,message.ehail_fee,message.improvement_surcharge,
            message.total_amount,message.payment_type,message.trip_type,message.congestion_surcharge
        )
        .to_string()
    }
}
