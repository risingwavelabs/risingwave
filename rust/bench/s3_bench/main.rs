use std::collections::hash_map::{Entry, HashMap};
use std::ops::Div;
use std::str::FromStr;
use std::time::{Duration, Instant};

extern crate bytesize;

use bytesize::ByteSize;
use clap::Parser;
use futures::stream::{self, StreamExt};
use futures::{future, Future, FutureExt, TryFutureExt};
use itertools::Itertools;
use log::debug;
use rand::{Rng, SeedableRng};
use rusoto_core::{ByteStream, HttpClient, Region};
use rusoto_credential::ChainProvider;
use rusoto_s3::*;
use tokio::io::AsyncReadExt;

const READ_BUFFER_SIZE: ByteSize = ByteSize::mib(8);

fn gen_obj(size: ByteSize) -> Vec<u8> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    let mut buffer: Vec<u8> = vec![0; size.as_u64() as usize];
    rng.fill(&mut buffer[..]);
    buffer
}

async fn put(cfg: &Config, client: &S3Client, name: String, obj: Vec<u8>) {
    let req = PutObjectRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name,
        body: Some(obj.into()),
        ..Default::default()
    };
    client.put_object(req).await.unwrap();
}

async fn multi_part_upload(
    cfg: &Config,
    client: &S3Client,
    name: String,
    obj: Vec<u8>,
    part_size: ByteSize,
) {
    let t_create_multi_part_upload = Instant::now();
    let req = CreateMultipartUploadRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        ..Default::default()
    };
    let rsp = client.create_multipart_upload(req).await.unwrap();
    let upload_id = rsp.upload_id.unwrap();
    debug!(
        "create multi part upload: {:?}",
        t_create_multi_part_upload.elapsed()
    );

    let name_clone = name.clone();
    let upload_id_clone = upload_id.clone();
    let create_upload_part_req = move |part: Vec<u8>, part_number: i64| UploadPartRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name_clone.clone(),
        upload_id: upload_id_clone.clone(),
        part_number,
        body: Some(part.into()),
        ..Default::default()
    };

    let t_partition = Instant::now();
    let chunks = obj
        .chunks(part_size.as_u64() as usize)
        .into_iter()
        .map(|chunk| chunk.to_owned())
        .collect_vec();
    debug!("partition obj: {:?}", t_partition.elapsed());

    let t_multi_part_upload = Instant::now();
    let futures = chunks
        .into_iter()
        .enumerate()
        .map(|(i, part)| create_upload_part_req(part, (i + 1) as i64))
        .map(|req| client.upload_part(req))
        .collect_vec();
    let completed_parts = future::try_join_all(futures)
        .await
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(i, rsp)| CompletedPart {
            e_tag: rsp.e_tag,
            part_number: Some((i + 1) as i64),
        })
        .collect_vec();
    debug!("multi part upload: {:?}", t_multi_part_upload.elapsed());

    let t_complete_multi_part_upload = Instant::now();
    let req = CompleteMultipartUploadRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        upload_id: upload_id.clone(),
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(completed_parts),
        }),
        ..Default::default()
    };
    client.complete_multipart_upload(req).await.unwrap();
    debug!(
        "complete multi part upload: {:?}",
        t_complete_multi_part_upload.elapsed()
    );
}

async fn get(cfg: &Config, client: &S3Client, name: String) {
    let req = GetObjectRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        ..Default::default()
    };
    let body = client.get_object(req).await.unwrap().body.unwrap();
    let bytes = read_byte_stream(body).await;
    debug!(
        "read size: {}",
        ByteSize::b(bytes as u64).to_string_as(true)
    )
}

async fn read_byte_stream(stream: ByteStream) -> usize {
    let mut stream = stream.into_async_read();
    let mut buf = vec![0; READ_BUFFER_SIZE.as_u64() as usize];
    let mut read = 0;
    loop {
        match stream.read(&mut buf).await.unwrap() {
            0 => break,
            x => read += x,
        }
    }
    read
}

async fn multi_part_get(cfg: &Config, client: &S3Client, name: String, part_numbers: Vec<i64>) {
    let create_part_get_req = move |part_number| GetObjectRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        part_number: Some(part_number),
        ..Default::default()
    };
    let futures = part_numbers
        .into_iter()
        .map(create_part_get_req)
        .map(|req| client.get_object(req))
        .map(|f| f.and_then(|rsp| async move { Ok(read_byte_stream(rsp.body.unwrap()).await) }))
        .collect_vec();
    let bytes: usize = future::try_join_all(futures).await.unwrap().iter().sum();
    debug!(
        "read size: {}",
        ByteSize::b(bytes as u64).to_string_as(true)
    )
}

async fn byte_range_get(cfg: &Config, client: &S3Client, name: String, start: u64, end: u64) {
    let req = GetObjectRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        range: Some(format!("bytes={}-{}", start, end)),
        ..Default::default()
    };
    let body = client.get_object(req).await.unwrap().body.unwrap();
    let bytes = read_byte_stream(body).await;
    debug!(
        "read size: {}",
        ByteSize::b(bytes as u64).to_string_as(true)
    )
}

// Avoid regenerate objs in the same size with `rand`.
#[derive(Default)]
struct ObjPool(HashMap<ByteSize, Vec<u8>>);

impl ObjPool {
    fn obj(&mut self, size: ByteSize) -> Vec<u8> {
        match self.0.entry(size) {
            Entry::Occupied(o) => o.get().to_owned(),
            Entry::Vacant(v) => v.insert(gen_obj(size)).to_owned(),
        }
    }
}

fn de_size_str<'de, D>(deserializer: D) -> Result<ByteSize, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StringVisitor;

    impl<'de> serde::de::Visitor<'de> for StringVisitor {
        type Value = ByteSize;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("ByteSize")
        }

        fn visit_str<E>(self, value: &str) -> Result<ByteSize, E>
        where
            E: serde::de::Error,
        {
            Ok(FromStr::from_str(value).unwrap())
        }
    }

    deserializer.deserialize_any(StringVisitor)
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", content = "args")]
enum Case {
    Put {
        name: String,
        obj: String,
        #[serde(deserialize_with = "de_size_str")]
        size: ByteSize,
    },
    MultiPartUpload {
        name: String,
        obj: String,
        #[serde(deserialize_with = "de_size_str")]
        size: ByteSize,
        #[serde(deserialize_with = "de_size_str")]
        part: ByteSize,
    },
    Get {
        name: String,
        obj: String,
    },
    MultiPartGet {
        name: String,
        obj: String,
        part: Vec<i64>,
    },
    ByteRangeGet {
        name: String,
        obj: String,
        start: u64,
        end: u64,
    },
}

struct Cost {
    execution_time_min: Duration,
    execution_time_max: Duration,
    execution_time_avg: Duration,
    execution_time_p50: Duration,
    execution_time_p90: Duration,
    execution_time_p99: Duration,
}

async fn exec<F, FB>(fb: FB) -> Duration
where
    F: Future,
    FB: FnOnce() -> F + Clone,
{
    let start = Instant::now();
    fb().await;
    start.elapsed()
}

async fn iter_exec<F, FB>(fb: FB, iter_size: usize) -> Cost
where
    F: Future,
    FB: FnOnce() -> F + Clone,
{
    let mut durations = stream::iter(0..iter_size)
        .flat_map(|_| exec(fb.clone()).into_stream())
        .collect::<Vec<_>>()
        .await;
    durations.sort();
    Cost {
        execution_time_min: durations.first().unwrap().to_owned(),
        execution_time_max: durations.last().unwrap().to_owned(),
        execution_time_avg: durations
            .iter()
            .sum::<Duration>()
            .div(durations.len() as u32),
        execution_time_p50: durations
            .get((iter_size as i32 / 2 - 1) as usize)
            .unwrap_or(&Duration::from_nanos(0))
            .to_owned(),
        execution_time_p90: durations
            .get((iter_size as i32 / 10 * 9 - 1) as usize)
            .unwrap_or(&Duration::from_nanos(0))
            .to_owned(),
        execution_time_p99: durations
            .get((iter_size as i32 / 100 * 99 - 1) as usize)
            .unwrap_or(&Duration::from_nanos(0))
            .to_owned(),
    }
}

async fn run_case(case: Case, cfg: &Config, client: &S3Client, objs: &mut ObjPool) {
    let (name, cost) = match case {
        Case::Put {
            name,
            obj: obj_name,
            size: obj_size,
        } => {
            let obj = objs.obj(obj_size);
            (
                name.to_owned(),
                iter_exec(|| put(cfg, client, obj_name, obj), cfg.iter).await,
            )
        }
        Case::MultiPartUpload {
            name,
            obj: obj_name,
            size: obj_size,
            part: part_size,
        } => {
            let obj = objs.obj(obj_size);
            (
                name.to_owned(),
                iter_exec(
                    || multi_part_upload(cfg, client, obj_name, obj, part_size),
                    cfg.iter,
                )
                .await,
            )
        }
        Case::Get {
            name,
            obj: obj_name,
        } => (
            name.to_owned(),
            iter_exec(|| get(cfg, client, obj_name), cfg.iter).await,
        ),
        Case::MultiPartGet {
            name,
            obj: obj_name,
            part: part_numbers,
        } => (
            name.to_owned(),
            iter_exec(
                || multi_part_get(cfg, client, obj_name, part_numbers),
                cfg.iter,
            )
            .await,
        ),
        Case::ByteRangeGet {
            name,
            obj: obj_name,
            start,
            end,
        } => (
            name.to_owned(),
            iter_exec(
                || byte_range_get(cfg, client, obj_name, start, end),
                cfg.iter,
            )
            .await,
        ),
    };
    let d2s = |d: &Duration| match d.as_nanos() {
        0 => "-".to_owned(),
        _ => format!("{:.3?}", d),
    };
    if cfg.format {
        println!(
            "{} ({} iters),{},{},{},{},{},{}",
            name,
            cfg.iter,
            d2s(&cost.execution_time_avg),
            d2s(&cost.execution_time_min),
            d2s(&cost.execution_time_max),
            d2s(&cost.execution_time_p50),
            d2s(&cost.execution_time_p90),
            d2s(&cost.execution_time_p99),
        );
        return;
    }
    println!("{} ({} iters)", name, cfg.iter);
    println!("  lat-avg: {}", d2s(&cost.execution_time_avg));
    println!("  lat-min: {}", d2s(&cost.execution_time_min));
    println!("  lat-max: {}", d2s(&cost.execution_time_max));
    println!("  lat-p50: {}", d2s(&cost.execution_time_p50));
    println!("  lat-p90: {}", d2s(&cost.execution_time_p90));
    println!("  lat-p99: {}", d2s(&cost.execution_time_p99));
    println!();
}

fn read_cases(cfg: &Config) -> Vec<Case> {
    let data = std::fs::read_to_string(&cfg.path).unwrap();
    toml::from_str::<HashMap<String, Vec<Case>>>(&data)
        .unwrap()
        .remove("case")
        .unwrap()
}

#[derive(Parser, Debug)]
pub struct Config {
    /// AWS S3 Region.
    #[clap(short, long)]
    region: String,

    /// AWS S3 Bucket, either <BUCKET> or <ENDPOINT> should be given.
    #[clap(short, long)]
    bucket: Option<String>,

    /// AWS S3 Endpoint, either <BUCKET> or <ENDPOINT> should be given.
    /// Format: <AccessPointName>-<AccountId>.s3-accesspoint.<Region>.amazonaws.com[.cn]
    #[clap(short, long)]
    endpoint: Option<String>,

    /// Iter times per case.
    #[clap(short, long)]
    iter: usize,

    /// Path to cases toml file.
    #[clap(short, long)]
    path: String,

    /// Print tabular format.
    #[clap(short, long)]
    format: bool,
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let mut cfg = Config::parse();

    let region = match (&cfg.bucket, &cfg.endpoint) {
        (Some(_), None) => Region::from_str(&cfg.region).unwrap(),
        (None, Some(access_point)) => {
            cfg.bucket = Some("".to_owned());
            Region::Custom {
                name: cfg.region.to_owned(),
                endpoint: access_point.to_owned(),
            }
        }
        _ => panic!("Either <BUCKET> or <ENDPOINT> must be given."),
    };

    let provider = ChainProvider::new();
    let client = S3Client::new_with(HttpClient::new().unwrap(), provider, region);
    let mut objs = ObjPool::default();

    let cases = read_cases(&cfg);

    if cfg.format {
        println!("name,lat-avg,lat-min,lat-max,lat-p50,lat-p90,lat-p99")
    }
    for case in cases {
        debug!("running case: {:?}", case);
        run_case(case, &cfg, &client, &mut objs).await;
    }
}
