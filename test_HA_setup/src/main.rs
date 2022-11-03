// Updated example from http://rosettacode.org/wiki/Hello_world/Web_server#Rust
// to work with Rust 1.0 beta

use etcd_client::{Client as EtcdClient, Error};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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

// etcd endpoint is 2379 via simpleweb-etcd
// prometheus-kube-prometheus-kube-etcd only exposes metrics
async fn test_etcd() -> Result<(), Error> {
    let endpoints = vec![String::from("127.0.0.1:2379")];
    let mut client = EtcdClient::connect(endpoints, None).await?;
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

fn handle_write(mut stream: TcpStream) {
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
    let _ = test_etcd().await;


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
