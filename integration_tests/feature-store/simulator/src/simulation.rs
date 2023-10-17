use std::ops::DerefMut;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use futures::future::join_all;
use rand::Rng;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::server_pb::server_client::ServerClient;
use crate::server_pb::StartTrainingRequest;
use crate::{entities, entities_taxi};

fn get_delay_mills(delay_val: f64) -> u64 {
    let turbulence = rand::thread_rng().gen_range((delay_val * 0.6)..(delay_val * 1.1));
    (turbulence * 10000.0) as u64
}

pub async fn main_loop(simulator_type: String) {
    let client = Arc::new(Mutex::new(
        ServerClient::connect("https://localhost:2666")
            .await
            .expect("failed to connect to feature store server"),
    ));
    println!("Connected to server");
    match simulator_type.as_str() {
        "taxi" => mock_taxi(client).await,
        "mfa" => mock_user_mfa(client).await,
        _ => panic!("Only taxi and mfa"),
    }
}

async fn mock_taxi(client: Arc<Mutex<ServerClient<Channel>>>) {
    let (offline_features, online_features) = entities_taxi::parse_taxi_metadata();
    println!("Write training data len is {:?}", offline_features.len());
    let mut threads = vec![];
    for fea in offline_features {
        let client_mutex = client.clone();
        let handle = tokio::spawn(async move {
            fea.mock_act(client_mutex.lock().await.deref_mut())
                .await
                .unwrap();
        });
        threads.push(handle);
    }
    join_all(threads).await;

    println!("Start training");

    sleep(Duration::from_millis(1000));

    client
        .lock()
        .await
        .deref_mut()
        .start_training(StartTrainingRequest {})
        .await
        .unwrap();

    println!("Offline feature has been written to written in kafka");
    let mut threads = vec![];
    for fea in online_features {
        let client_mutex = client.clone();
        let handle = tokio::spawn(async move {
            fea.mock_act(client_mutex.lock().await.deref_mut())
                .await
                .unwrap();
            println!(
                "write online feature, DOLocationID is {:?}",
                fea.dolocation_id
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let fare_amount = fea
                .mock_get_amount(client_mutex.lock().await.deref_mut())
                .await;
            println!(
                "DOLocationID is {:?} fare amount: predicted results {:?} , real results {:?}",
                fea.dolocation_id, fare_amount, fea.fare_amount
            );
        });
        threads.push(handle);
    }
    join_all(threads).await;
}

#[allow(dead_code)]
async fn mock_user_mfa(client: Arc<Mutex<ServerClient<Channel>>>) {
    let users = entities::parse_user_metadata().unwrap();
    let mut threads = vec![];
    for user in users {
        let client_mutex = client.clone();
        let handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(get_delay_mills(
                    1.0 / user.activeness,
                )));
                let history = user
                    .mock_act(client_mutex.lock().await.deref_mut())
                    .await
                    .unwrap();
                println!(
                    "fire action success: {}",
                    serde_json::to_string(&history).unwrap()
                );

                sleep(Duration::from_millis(200));
                let (count, sum) = user
                    .mock_get_feature(client_mutex.lock().await.deref_mut())
                    .await;
                println!("userid {} , count: {:?} sum {:?}", user.userid, count, sum);
            }
        });
        threads.push(handle);
    }
    join_all(threads).await;
}
