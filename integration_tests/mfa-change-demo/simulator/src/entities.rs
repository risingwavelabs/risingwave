use serde_derive::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use rand::Rng;
use tonic::transport::Channel;
use crate::server::server_client::ServerClient;
use crate::server::{GetFeatureRequest, ActionType, ReportActionRequest};

///    "address_lat": partial(np.random.uniform, low=-180, high=180),
//     "address_long": partial(np.random.uniform, low=-180, high=180),
//     "age_approx": partial(np.random.randint, low=18, high=100),
//     "gender": partial(np.random.choice, [0, 1]),
//     "occupation": partial(np.random.randint, low=0, high=25),

#[derive(Serialize, Deserialize)]
pub struct User {
    pub(crate) userid: String,
    pub(crate) address_lat: f64,
    pub(crate) address_long: f64,
    pub (crate) activeness: f64,
    #[serde(skip)]
    pub(crate) context: UserContext,
    pub(crate)  age_approx: f64,
    pub(crate)  gender: f64,
    pub(crate) occupation: f64
}


#[derive(Serialize, Deserialize)]
pub struct ActionHistory{
    userid: String,
    event_type: i32,
    changenum: i64,
    timestamp: u64
}

#[derive(Default)]
#[derive(Serialize, Deserialize)]
pub struct UserContext {
    userid: String,

    // sql source: recent purchased items / item -> user reversed
    recent_ratings: f64,
    recent_brand: f64,
    recent_type_: f64,
    recent_freshness: f64,

    global_ratings: f64,
    global_brand: f64,
    global_type_: f64,
    global_freshness: f64,

    conversion_count: i32
}



pub trait UpdatableContext{
    fn update(&self, record: &ActionHistory);
}


impl User{
    pub(crate) async fn mock_act<'a>(&'a self, client: &'a mut ServerClient<Channel>) -> Result<ActionHistory, &str> {
        // json.insert("item", generated item)
        // json.insertion
        let changenum:i64 = rand::thread_rng().gen_range(0, 90);
        let (changenum,event_type) = {
            if changenum > 0 && changenum < 30 {
                (changenum,ActionType::Mfachangeadd)
            }else if changenum < 60{
                (changenum - 30,ActionType::Mfachangereduce)
            }else{
                (0,ActionType::Other)
            }
        };
        let response = client.report_action(tonic::Request::new(
            ReportActionRequest {
                userid: self.userid.clone(),
                eventtype: event_type as i32,
                changenum,
            })).await.unwrap();
        let timestamp = response.into_inner().timestamp;

        Ok(ActionHistory{
            userid: self.userid.clone(),
            changenum: changenum,
            event_type: event_type as i32,
            timestamp
        })
    }

    pub async fn mock_get_feature(&self, client: &mut ServerClient<Channel>) -> (u64,i64) {
        let response = client.get_feature(GetFeatureRequest{
            userid: self.userid.clone()
        })
            .await
            .unwrap();

            let inner = response.into_inner();
        (inner.count,inner.sum)
    }
}


pub(crate) fn read_users_json(path: PathBuf) -> Result<Vec<User>, Box<dyn Error>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let users: Vec<User> = serde_json::from_reader(reader)?;
    Ok(users)
}


pub fn parse_user_metadata() -> Result<Vec<User>, ()> {
    let generator_path = std::env::var("GENERATOR_PATH")
        .unwrap_or("../generator".to_string());

    let users = read_users_json(
        Path::new(&*generator_path)
                .join("users.json")
    ).unwrap();

    return Ok(users);
}
