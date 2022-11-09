// Updated example from http://rosettacode.org/wiki/Hello_world/Web_server#Rust
// to work with Rust 1.0 beta

use etcd_client::{
    Client as EtcdClient, ConnectOptions, Error, LeaderResponse, ProclaimOptions, ResignOptions,
};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use std::{env, thread};

fn handle_read(mut stream: &TcpStream) {
    let mut buf = [0u8; 4096];
    match stream.read(&mut buf) {
        Ok(_) => {
            let req_str = String::from_utf8_lossy(&buf);
            println!("{}", req_str);
        }
        Err(e) => println!("Unable to read stream: {}", e),
    }
}

/*
Check if remote port is open
    nc -zvw10 simpleweb-etcd 2388
Check if dns entry exists
    nslookup simpleweb-etcd

etcd docs
    https://etcd.io/docs/v3.3/dev-guide/api_concurrency_reference_v3/
*/

// may give you connection issue if you service is not ready yet
async fn get_client() -> Result<EtcdClient, Error> {
    let opts =
        ConnectOptions::default().with_keep_alive(Duration::from_secs(3), Duration::from_secs(5));

    // k get endpoints
    let endpoints = vec![String::from("simpleweb-etcd:2388")];
    let client = EtcdClient::connect(endpoints, Some(opts)).await?;
    Ok(client)
}

async fn test_etcd() -> Result<(), Error> {
    let mut client = get_client().await?;

    // put kv
    println!("putting kv in etcd...");
    client.put("foo", "bar", None).await?;
    println!("done putting");

    // get kv
    println!("getting kv from etcd...");
    let resp = client.get("foo", None).await?;
    if let Some(kv) = resp.kvs().first() {
        println!("got kv: {{{}: {}}}", kv.key_str()?, kv.value_str()?);
    } else {
        println!("failed to get kv pair");
    }
    Ok(())
}

async fn elect_test() -> Result<(), Error> {
    println!("calling elect_test");

    // How do the other servers know that they are also part of the rust thing?
    // I think right now I just have 3 clusters with 1 node each

    let pod_name_res = env::var("OUT".to_string());
    let mut pod_name = "Unknown".to_string();
    if pod_name_res.is_ok() {
        pod_name = pod_name_res.unwrap();
    }

    let mut client = get_client().await?;

    loop {
        // retry getting a lease
        let mut resp_res = client.lease_grant(5, None).await;
        while resp_res.is_err() {
            thread::sleep(Duration::from_secs(1));
            resp_res = client.lease_grant(5, None).await;
        }
        let resp = resp_res.unwrap();
        let lease_id = resp.id();
        println!("grant ttl: {:?} (in s), id: {:?}", resp.ttl(), resp.id());

        // campaign
        let resp = client
            .campaign("myElection", pod_name.clone(), lease_id)
            .await?;
        let mut leader = resp.leader().unwrap();
        println!(
            "election name: {:?}, leaseId: {:?}",
            leader.name_str(),
            leader.lease()
        );

        // // proclaim
        // let resp = client
        //     .proclaim(
        //         "123",
        //         Some(ProclaimOptions::new().with_leader(leader.clone())),
        //     )
        //     .await?;
        // let header = resp.header();
        // println!("proclaim header {:?}", header.unwrap());

        // observe the latest leader. Retry to get valid observer
        let mut msg_res = client.observe(leader.name()).await;
        while msg_res.is_err() {
            thread::sleep(Duration::from_secs(1));
            msg_res = client.observe(leader.name()).await;
        }
        let mut res = msg_res.unwrap();

        loop {
            // ignore errors
            let tmp_res_opt = res.message().await;
            if tmp_res_opt.is_err() {
                continue;
            }

            // detect if leader failed
            let tmp_opt = tmp_res_opt.unwrap();
            if tmp_opt.is_none() {
                println!("got leader result 'None'. Trying to get elected");
                break;
            }

            // output current leader
            let actual_msg = tmp_opt.unwrap();
            println!("observe key {:?}", actual_msg.kv().unwrap().key_str());
            if let Some(kv) = actual_msg.kv() {
                println!(
                    "key: {:?}, val: {:?}, version: {:?}",
                    kv.key_str(),
                    kv.value_str(),
                    kv.version()
                );
            }
        }
    }

    // leader
   // let resp = client.leader("myElection").await?;
   // let kv = resp.kv().unwrap();
   // println!("key is {:?}", kv.key_str());
   // println!("value is {:?}", kv.value_str());

   // // I think we do not need this part
   // // resign
   // let resign_option = ResignOptions::new().with_leader(leader.clone());
   // let resp = client.resign(Some(resign_option)).await?;
   // let header = resp.header();
   // println!("resign header {:?}", header.unwrap());

    Ok(())
}

fn handle_write(mut stream: TcpStream) {
    println!("in handle_write");

    // return environment var or default
    let out = env::var("OUT").unwrap_or("unknown".to_string());
    let leader = "unknown".to_string();
    let r = format!("HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n<html><body>I am {out}</br>Leader is {leader}</body></html>\r\n");
    match stream.write(r.as_bytes()) {
        Ok(_) => println!("Response sent"),
        Err(e) => println!("Failed sending response: {}", e),
    }
}

fn handle_client(stream: TcpStream) {
    handle_read(&stream);
    handle_write(stream);
}

#[tokio::main]
async fn main() {
    println!("testing etcd...");
    let res = test_etcd().await;
    if res.is_err() {
        println!("test etcd res is {:?}", res.err().unwrap())
    }

    let res = elect_test().await;
    if res.is_err() {
        println!("elect test res is {:?}", res.err().unwrap())
    }

    let port = 8080;
    let listener = TcpListener::bind(format!("127.0.0.1:{port}")).unwrap();
    println!("Listening for connections on port {}", port);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| handle_client(stream));
            }
            Err(e) => {
                println!("Unable to connect: {}", e);
            }
        }
    }
}
