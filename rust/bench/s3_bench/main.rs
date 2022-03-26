// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

async fn put(cfg: &Config, client: &S3Client, name: String, obj: Vec<u8>) -> Cost {
    let t = Instant::now();
    let bytes = obj.len();
    let req = PutObjectRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name,
        body: Some(obj.into()),
        ..Default::default()
    };
    let ttfb = t.elapsed();
    client.put_object(req).await.unwrap();
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn multi_part_upload(
    cfg: &Config,
    client: &S3Client,
    name: String,
    obj: Vec<u8>,
    part_size: ByteSize,
) -> Cost {
    let t = Instant::now();
    let bytes = obj.len();
    let req = CreateMultipartUploadRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        ..Default::default()
    };
    let rsp = client.create_multipart_upload(req).await.unwrap();
    let upload_id = rsp.upload_id.unwrap();

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

    let chunks = obj
        .chunks(part_size.as_u64() as usize)
        .into_iter()
        .map(|chunk| chunk.to_owned())
        .collect_vec();

    let futures = chunks
        .into_iter()
        .enumerate()
        .map(|(i, part)| create_upload_part_req(part, (i + 1) as i64))
        .map(|req| client.upload_part(req))
        .collect_vec();
    let ttfb = t.elapsed();
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
    let ttlb = t.elapsed();

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
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn get(cfg: &Config, client: &S3Client, name: String) -> Cost {
    let t = Instant::now();
    let req = GetObjectRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        ..Default::default()
    };
    let body = client.get_object(req).await.unwrap().body.unwrap();
    let ttfb = t.elapsed();
    let bytes = read_byte_stream(body).await;
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn multi_part_get(
    cfg: &Config,
    client: &S3Client,
    name: String,
    part_numbers: Vec<i64>,
) -> Cost {
    let t = Instant::now();
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
        .map(|f| {
            f.and_then(
                |rsp| async move { Ok((read_byte_stream(rsp.body.unwrap()).await, t.elapsed())) },
            )
        })
        .collect_vec();
    let (bytes, ttfb) = future::try_join_all(futures)
        .await
        .unwrap()
        .iter()
        .fold((0, Duration::MAX), |(total_bytes, ttfb), (bytes, time)| {
            (total_bytes + bytes, ttfb.min(*time))
        });
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
}

async fn byte_range_get(
    cfg: &Config,
    client: &S3Client,
    name: String,
    start: u64,
    end: u64,
) -> Cost {
    let t = Instant::now();
    let req = GetObjectRequest {
        bucket: cfg.bucket.clone().unwrap(),
        key: name.clone(),
        range: Some(format!("bytes={}-{}", start, end)),
        ..Default::default()
    };
    let body = client.get_object(req).await.unwrap().body.unwrap();
    let ttfb = t.elapsed();
    let bytes = read_byte_stream(body).await;
    let ttlb = t.elapsed();
    Cost {
        bytes,
        ttfb,
        ttlb,
        ..Default::default()
    }
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

#[derive(Debug, Clone, serde::Deserialize)]
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

#[derive(Default)]
struct Cost {
    /// total bytes
    bytes: usize,
    /// time to first byte
    ttfb: Duration,
    /// time to last byte
    ttlb: Duration,
    /// time to live
    ttl: Duration,
}

struct Durations {
    min: Duration,
    max: Duration,
    avg: Duration,
    p50: Duration,
    p90: Duration,
    p99: Duration,
}

impl Durations {
    fn gen(mut durations: Vec<Duration>) -> Durations {
        durations.sort();
        Durations {
            min: durations.first().unwrap().to_owned(),
            max: durations.last().unwrap().to_owned(),
            avg: durations
                .iter()
                .sum::<Duration>()
                .div(durations.len() as u32),
            p50: durations
                .get((durations.len() as i32 / 2 - 1) as usize)
                .unwrap_or(&Duration::from_nanos(0))
                .to_owned(),
            p90: durations
                .get((durations.len() as i32 / 10 * 9 - 1) as usize)
                .unwrap_or(&Duration::from_nanos(0))
                .to_owned(),
            p99: durations
                .get((durations.len() as i32 / 100 * 99 - 1) as usize)
                .unwrap_or(&Duration::from_nanos(0))
                .to_owned(),
        }
    }
}

struct Analysis {
    bytes: ByteSize,
    bandwidth: ByteSize,
    rtts: Durations,
    ttfbs: Durations,
}

async fn exec<F, FB>(fb: FB) -> Cost
where
    F: Future<Output = Cost>,
    FB: FnOnce() -> F + Clone,
{
    let start = Instant::now();
    let mut cost = fb().await;
    cost.ttl = start.elapsed();
    cost
}

async fn iter_exec<F, FB>(fb: FB, iter_size: usize) -> Analysis
where
    F: Future<Output = Cost>,
    FB: FnOnce() -> F + Clone,
{
    let costs = stream::iter(0..iter_size)
        .flat_map(|_| exec(fb.clone()).into_stream())
        .collect::<Vec<_>>()
        .await;
    let bytes = ByteSize::b(costs[0].bytes as u64);
    let ttls = Durations::gen(costs.iter().map(|cost| cost.ttl).collect_vec());
    let ttfbs = Durations::gen(costs.iter().map(|cost| cost.ttfb).collect_vec());
    // let ttlbs = Durations::gen(costs.iter().map(|cost| cost.ttlb).collect_vec());
    let data_transfer_secs = costs
        .iter()
        .map(|cost| (cost.ttlb - cost.ttfb).as_secs_f64())
        .collect_vec();
    let data_transfer_sec_avg =
        data_transfer_secs.iter().sum::<f64>() / data_transfer_secs.len() as f64;
    let bandwidth = ByteSize::b(((costs[0].bytes as f64) / data_transfer_sec_avg) as u64);
    Analysis {
        bytes,
        bandwidth,
        rtts: ttls,
        ttfbs,
    }
}

async fn run_case(index: usize, case: Case, cfg: &Config, client: &S3Client, objs: &mut ObjPool) {
    let (name, analysis) = match case.clone() {
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
    let d2s_with_case = |case: &Case, d: &Duration| match case {
        Case::Put { .. } | Case::MultiPartGet { .. } => "-".to_owned(),
        _ => d2s(d),
    };
    let data_with_name = [
        ("name".to_owned(), format!("{} ({} iters)", name, cfg.iter)),
        ("bytes".to_owned(), analysis.bytes.to_string_as(true)),
        (
            "bandwidth".to_owned(),
            analysis.bandwidth.to_string_as(true),
        ),
        ("rtt-avg".to_owned(), d2s(&analysis.rtts.avg)),
        ("rtt-min".to_owned(), d2s(&analysis.rtts.min)),
        ("rtt-max".to_owned(), d2s(&analysis.rtts.max)),
        ("rtt-p50".to_owned(), d2s(&analysis.rtts.p50)),
        ("rtt-p90".to_owned(), d2s(&analysis.rtts.p90)),
        ("rtt-p99".to_owned(), d2s(&analysis.rtts.p99)),
        (
            "ttfb-avg".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.avg),
        ),
        (
            "ttfb-min".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.min),
        ),
        (
            "ttfb-max".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.max),
        ),
        (
            "ttfb-p50".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.p50),
        ),
        (
            "ttfb-p90".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.p90),
        ),
        (
            "ttfb-p99".to_owned(),
            d2s_with_case(&case, &analysis.ttfbs.p99),
        ),
    ];
    if cfg.format {
        if index == 0 {
            let names = data_with_name
                .iter()
                .map(|(name, _)| name.to_owned())
                .collect_vec();
            println!("{}", names.join(","));
        }
        let data = data_with_name
            .iter()
            .map(|(_, item)| item.to_owned())
            .collect_vec();
        println!("{}", data.join(","));
        return;
    }
    for (name, item) in data_with_name {
        println!("{}: {}", name, item);
    }
    println!()
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

    let mut cases = read_cases(&cfg);

    for (i, case) in cases.drain(..).enumerate() {
        debug!("running case: {:?}", case);
        run_case(i, case, &cfg, &client, &mut objs).await;
    }
}
