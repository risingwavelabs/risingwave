use rand;
use std::ops::DerefMut;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use rand::Rng;
use futures::future::join_all;
use tokio::sync::{Mutex};
use crate::{entities};
use crate::server::server_client::ServerClient;

fn get_delay_mills(delay_val: f64) -> u64 {
    let turbulence = rand::thread_rng().gen_range((delay_val * 0.6) as f64, (delay_val * 1.1) as f64) as f64;
    (turbulence * 10000.0) as u64
}

pub async fn main_loop() {
    let users = entities::parse_user_metadata().unwrap();

    let client = Arc::new(Mutex::new(
        ServerClient::connect("https://127.0.0.1:2666")
            .await
            .expect("failed to connect to recommender server")));
    println!("Connected to server");

    let mut threads = vec![];
    for user in users {
        let client_mutex = client.clone();
        let handle = tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(get_delay_mills(1.0 / user.activeness)));
                let history = user.mock_act(client_mutex.lock().await.deref_mut())
                    .await
                    .unwrap();
                println!("fire action success: {}", serde_json::to_string(&history).unwrap());

                sleep(Duration::from_millis(200));
                let (count,sum) = user.mock_get_feature(client_mutex.lock().await.deref_mut())
                    .await;
                println!("userid {} , count: {:?} sum {:?}", user.userid,count,sum);
            }
        });
        threads.push(handle);
    }
    join_all(threads).await;
}