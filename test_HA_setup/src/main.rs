// Updated example from http://rosettacode.org/wiki/Hello_world/Web_server#Rust
// to work with Rust 1.0 beta

use etcd_client::{Client as EtcdClient, ConnectOptions, Error, ProclaimOptions, ResignOptions};
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

    let mut client = get_client().await?;
    let resp = client.lease_grant(10, None).await?;
    let lease_id = resp.id();
    println!("grant ttl:{:?}, id:{:?}", resp.ttl(), resp.id());

    // campaign
    // let resp = client.campaign("simple-web-leader", "pod-id", lease_id).await?;
    let resp = client.campaign("myElection", "123", lease_id).await?;
    let leader = resp.leader().unwrap(); // what is our leader name? 123?
    println!(
        "election name: {:?}, leaseId: {:?}",
        leader.name_str(),
        leader.lease()
    );

    // proclaim
    let resp = client
        .proclaim(
            "123",
            Some(ProclaimOptions::new().with_leader(leader.clone())),
        )
        .await?;
    let header = resp.header();
    println!("proclaim header {:?}", header.unwrap());

    // observe
    let mut msg = client.observe(leader.name()).await?;
    loop {
        if let Some(resp) = msg.message().await? {
            println!("observe key {:?}", resp.kv().unwrap().key_str());
            if resp.kv().is_some() {
                break;
            }
        }
    }

    // leader
    let resp = client.leader("myElection").await?;
    let kv = resp.kv().unwrap();
    println!("key is {:?}", kv.key_str());
    println!("value is {:?}", kv.value_str());

    // I think we do not need this part
    // resign
    let resign_option = ResignOptions::new().with_leader(leader.clone());
    let resp = client.resign(Some(resign_option)).await?;
    let header = resp.header();
    println!("resign header {:?}", header.unwrap());

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
        println!("err is {:?}", res.err().unwrap())
    }

    let res = elect_test().await;
    if res.is_err() {
        println!("election res is err: {:?}", res)
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
