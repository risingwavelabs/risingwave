use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::NoTls;
use tonic::{Response, Status};
use crate::model::{RecallRequest, RecallResponse};
use crate::server::ReportActionResponse;
use crate::{ModelClient, Recwave};

pub const  GET_COUNT_SQL: &str = "
select count from user_mfa_change_count where userid = $1 order by window_start desc limit 1;
";
pub const  GET_SUM_SQL: &str = "
select udf_sum from user_mfa_change_sum where userid = $1 order by window_start desc limit 1;
";

impl Recwave {
    pub async fn recall(&self, userid: String) -> Result<(u64,i64), Status> {
        let request = RecallRequest {
            userid,
        };

        //"dbname=dev user=root host=127.0.0.1 port=4566"
        let con = format!("dbname=dev user=root host=127.0.0.1 port=4566");
        let (client,connection) = tokio_postgres::connect(&con, NoTls).await.unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let data_count = client.query(GET_COUNT_SQL, &[&request.userid.clone()]).await.unwrap();
        let data_sum = client.query(GET_SUM_SQL, &[&request.userid.clone()]).await.unwrap();
        let count = if data_count.len() == 0 {
            0 as i64
        }else{
            data_count[0].get(0)
        };
        let sum = if data_sum.len() == 0 {
            0 as i64
        }else{
            data_sum[0].get(0)
        };
        Ok((count as u64, sum))
    }

}