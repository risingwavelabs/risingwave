use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use rand::Rng;
use serde::{Deserialize, Serialize};
use tonic::transport::Channel;

use crate::server_pb::server_client::ServerClient;
use crate::server_pb::{GetFeatureRequest, ReportActionRequest};

#[derive(Serialize, Deserialize)]
pub struct User {
    pub(crate) userid: String,
    pub(crate) address_lat: f64,
    pub(crate) address_long: f64,
    pub(crate) activeness: f64,
    pub(crate) age_approx: f64,
    pub(crate) gender: f64,
    pub(crate) occupation: f64,
}

#[derive(Serialize, Deserialize)]
pub struct ActionHistory {
    userid: String,
    event_type: String,
    changenum: i64,
    timestamp: u64,
}

pub trait UpdatableContext {
    fn update(&self, record: &ActionHistory);
}

impl User {
    pub(crate) async fn mock_act<'a>(
        &'a self,
        client: &'a mut ServerClient<Channel>,
    ) -> Result<ActionHistory, &str> {
        let changenum: i64 = rand::thread_rng().gen_range(0..90);
        let (changenum, event_type) = {
            if changenum > 0 && changenum < 30 {
                (changenum, "mfa+")
            } else if changenum < 60 {
                (changenum - 30, "mfa-")
            } else {
                (0, "other")
            }
        };
        let response = client
            .report_action(tonic::Request::new(ReportActionRequest {
                userid: self.userid.clone(),
                eventtype: event_type.to_string(),
                changenum,
            }))
            .await
            .unwrap();
        let timestamp = response.into_inner().timestamp;

        Ok(ActionHistory {
            userid: self.userid.clone(),
            changenum,
            event_type: event_type.to_string(),
            timestamp,
        })
    }

    #[allow(dead_code)]
    pub async fn mock_get_feature(&self, client: &mut ServerClient<Channel>) -> (u64, i64) {
        let response = client
            .get_feature(GetFeatureRequest {
                userid: self.userid.clone(),
            })
            .await
            .unwrap();

        let inner = response.into_inner();
        (inner.count, inner.sum)
    }
}

#[allow(dead_code)]
pub(crate) fn read_users_json(path: PathBuf) -> Result<Vec<User>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let users: Vec<User> = serde_json::from_reader(reader)?;
    Ok(users)
}

pub fn parse_user_metadata() -> Result<Vec<User>, ()> {
    let generator_path = std::env::var("GENERATOR_PATH").unwrap_or("../generator".to_string());

    let users = read_users_json(Path::new(&*generator_path).join("users.json")).unwrap();

    Ok(users)
}
