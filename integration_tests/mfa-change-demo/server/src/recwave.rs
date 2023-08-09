use std::time::{SystemTime, UNIX_EPOCH};

use tonic::{Request, Response, Status};

use crate::kafka::KafkaSink;
use crate::server::server_server::Server;
use crate::server::{
    GetFeatureRequest, GetFeatureResponse, ReportActionRequest, ReportActionResponse,
};

pub struct Recwave {
    pub(crate) kafka: KafkaSink,
}

#[tonic::async_trait]
impl Server for Recwave {
    async fn get_feature(
        &self,
        request: Request<GetFeatureRequest>,
    ) -> Result<Response<GetFeatureResponse>, Status> {
        let userid = request.into_inner().userid;
        println!("Recwave::get_feature: userid={}", userid);
        let (recall_count, sum) = self.recall(userid.clone()).await.unwrap();
        Ok(Response::new(GetFeatureResponse {
            count: recall_count,
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
}

impl Recwave {
    async fn mock_report_action(
        &self,
        message: &ReportActionRequest,
    ) -> Result<Response<ReportActionResponse>, Status> {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH);
        match duration {
            Ok(dur) => {
                let timestamp = dur.as_micros();
                let json = Self::create_sink_json(message, timestamp as u64);
                println!("timestamp: {}, payload: {}", timestamp, json.clone());
                self.kafka.send("0".to_string(), json).await;
                Ok(Response::new(ReportActionResponse {
                    timestamp: timestamp as u64,
                }))
            }
            Err(_e) => Err(Status::unknown("Failed to generate timestamp".to_string())),
        }
    }

    pub(crate) fn create_sink_json(message: &ReportActionRequest, timestamp: u64) -> String {
        format!(
            "{{\"userid\": {:?}, \"eventype\": {:?}, \"changenum\": {:?}, \"timestamp\": {:?}}}",
            message.userid, message.eventtype, message.changenum, timestamp
        )
        .to_string()
    }
}
