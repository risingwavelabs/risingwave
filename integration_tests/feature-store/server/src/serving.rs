use tokio_postgres::NoTls;
use tonic::{Response, Status};

use crate::model::model_client::ModelClient;
use crate::model::{GetAmountRequest, TrainingRequest};
use crate::server_pb::StartTrainingResponse;
use crate::FeatureStoreServer;

pub const GET_COUNT_SQL: &str = "
select count from user_mfa_change_count where userid = $1 order by window_start desc limit 1;
";
pub const GET_SUM_SQL: &str = "
select udf_sum from user_mfa_change_sum where userid = $1 order by window_start desc limit 1;
";

impl FeatureStoreServer {
    pub async fn get_mfa_feature_from_rw(&self, userid: String) -> Result<(u64, i64), Status> {
        let con = format!("dbname=dev user=root host=frontend-node-0 port=4566");
        let (client, connection) = tokio_postgres::connect(&con, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let data_count = client
            .query(GET_COUNT_SQL, &[&userid.clone()])
            .await
            .unwrap();
        let data_sum = client.query(GET_SUM_SQL, &[&userid.clone()]).await.unwrap();
        let count = if data_count.len() == 0 {
            0 as i64
        } else {
            data_count[0].get(0)
        };
        let sum = if data_sum.len() == 0 {
            0 as i64
        } else {
            data_sum[0].get(0)
        };
        Ok((count as u64, sum))
    }

    pub async fn do_training(&self) -> Result<Response<StartTrainingResponse>, Status> {
        let request = TrainingRequest {};
        let mut model_client = ModelClient::connect("http://localhost:8080")
            .await
            .expect("Failed to connect to model server");
        println!("Training");
        let response = model_client.training(request).await;
        match response {
            Ok(_resp) => Ok(Response::new(StartTrainingResponse {})),
            Err(e) => Err(e),
        }
    }

    pub async fn get_taxi_amount(&self, do_location_id: i64,pu_location_id: i64) -> Result<f32, Status> {
        let request = GetAmountRequest { do_location_id ,pu_location_id};
        let mut model_client = ModelClient::connect("http://localhost:8080")
            .await
            .expect("Failed to connect to model server");
        let response = model_client.get_amount(request).await;
        match response {
            Ok(resp) => Ok(resp.into_inner().amount),
            Err(e) => Err(e),
        }
    }
}
