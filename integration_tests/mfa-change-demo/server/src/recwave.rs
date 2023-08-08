use std::collections::HashMap;
use std::time::{UNIX_EPOCH, SystemTime, SystemTimeError, Duration};
use serde_json::Map;
use tonic::{Request, Response, Status};
use tonic::transport::Channel;
use crate::kafka::KafkaSink;
use crate::model::model_server::Model;
use crate::model::{ RecallRequest, RecallResponse};
use crate::ModelClient;
use crate::server::{GetFeatureRequest, GetFeatureResponse, ReportActionRequest, ReportActionResponse};
use crate::server::server_server::Server;

pub struct Recwave{
    pub(crate) kafka: KafkaSink,
    pub(crate) record_id: usize, 
    pub(crate) mock: bool, 
}

pub struct RecwaveModelClient {}


#[tonic::async_trait]
impl Server for Recwave {
    async fn get_feature(&self, request: Request<GetFeatureRequest>)
                                -> Result<Response<GetFeatureResponse>, Status> {
        let userid = request.into_inner().userid;
        println!("Recwave::get_feature: userid={}", userid);
        let (recall_count,sum) = self.recall(userid.clone()).await.unwrap();
        Ok(Response::new(GetFeatureResponse {
            count: recall_count,
            sum:sum
        }))
        // match recall_response {
        //     Ok(item_ids) => {
        //
        //     }
        //     Err(e) => {
        //         Err(e)
        //     }
        // }
    }

    async fn report_action(&self, request: Request<ReportActionRequest>)
        -> Result<Response<ReportActionResponse>, Status> {
        let message = request.into_inner();
        self.mock_report_action(&message).await
    }
}

impl Recwave{
    async fn mock_report_action(&self, message: &ReportActionRequest) -> Result<Response<ReportActionResponse>, Status> {
        let duration = SystemTime::now().duration_since(UNIX_EPOCH);
        // todo: format duration to SQL acceptable timestamp before SQLization
        // println!("received action from user `{}` on item `{}`", &message.userid, &message.itemid);
        match duration {
            Ok(dur) => {
                let timestamp = dur.as_micros();
                let json = Self::create_sink_json(message, timestamp as u64);
                println!("timestamp: {}, payload: {}", timestamp, json.clone());
                self.kafka.send("0".to_string(), json).await;
                Ok(Response::new(ReportActionResponse {
                    timestamp: timestamp as u64
                }))
            }
            Err(e) => {
                Err(Status::unknown("Failed to generate timestamp".to_string()))
            }
        }
    }

    fn mock_get_feature(userid: String) -> Result<Response<GetFeatureResponse>, Status> {
        // println!("{} requested recommendations", userid);
        Ok(Response::new(GetFeatureResponse {
            count: 0,
            sum: 0
        }))
    }


    pub(crate) fn create_sink_json(message: &ReportActionRequest, timestamp: u64) -> String {
        format!("{{\"userid\": {:?}, \"eventype\": {:?}, \"changenum\": {:?}, \"timestamp\": {:?}}}",
            message.userid, message.eventtype, message.changenum, timestamp).to_string()
    }
}