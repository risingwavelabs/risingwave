// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::{self, BufRead, Write};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use reqwest::blocking::{Client, Response};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const STREAM_NAME: &str = "rw_records_demo.records";

#[derive(Parser, Debug)]
#[command(
    version,
    about = "CLI client for the RisingWave records HTTP demo",
    long_about = None
)]
struct Cli {
    /// Base URL of the demo HTTP service.
    #[arg(
        long,
        env = "RW_RECORDS_DEMO_URL",
        default_value = "http://127.0.0.1:4560/demo"
    )]
    url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Append one record.
    Append {
        /// Record body to append.
        body: String,
    },
    /// Run an interactive append prompt.
    AppendRepl,
    /// Read records from an inclusive sequence position.
    Read {
        /// Inclusive lower bound of seq_num.
        #[arg(long, default_value = "0")]
        seq_num: String,

        /// Maximum number of records to return.
        #[arg(long, default_value_t = 10)]
        limit: u32,
    },
    /// Read the latest visible record.
    Latest,
    /// Follow the stream in a terminal loop.
    Watch {
        /// Start following from an inclusive sequence number. If omitted, start from the latest visible record.
        #[arg(long)]
        seq_num: Option<String>,

        /// Maximum number of records to print per poll.
        #[arg(long, default_value_t = 100)]
        limit: u32,

        /// Poll interval in milliseconds.
        #[arg(long, default_value_t = 500)]
        interval_ms: u64,
    },
    /// Read the current tail token.
    Tail,
}

#[derive(Serialize)]
struct AppendRequest<'a> {
    body: &'a str,
}

#[derive(Debug, Deserialize, Serialize)]
struct TailResponse {
    seq_num: String,
    ts_ms: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct DemoRecord {
    seq_num: String,
    ts_ms: i64,
    body: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ReadResponse {
    records: Vec<DemoRecord>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let client = Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    let json = match &cli.command {
        Commands::Append { body } => json!(append_record(&client, &cli.url, body)?),
        Commands::AppendRepl => {
            append_repl(&client, &cli.url)?;
            return Ok(());
        }
        Commands::Read { seq_num, limit } => json!(fetch_read(&client, &cli.url, seq_num, *limit)?),
        Commands::Latest => {
            if let Some(record) = fetch_latest(&client, &cli.url)? {
                json!({ "records": [record] })
            } else {
                json!({ "records": [] })
            }
        }
        Commands::Watch {
            seq_num,
            limit,
            interval_ms,
        } => {
            watch(&client, &cli.url, seq_num.clone(), *limit, *interval_ms)?;
            return Ok(());
        }
        Commands::Tail => json!(fetch_tail(&client, &cli.url)?),
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&json).context("failed to render JSON output")?
    );
    Ok(())
}

fn endpoint(base_url: &str, path: &str) -> String {
    format!("{}{}", base_url.trim_end_matches('/'), path)
}

fn append_record(client: &Client, base_url: &str, body: &str) -> Result<DemoRecord> {
    read_typed(
        client
            .post(endpoint(base_url, "/records"))
            .json(&AppendRequest { body })
            .send()
            .context("append request failed")?,
    )
}

fn append_repl(client: &Client, base_url: &str) -> Result<()> {
    println!("Connected to {} via {}", STREAM_NAME, base_url);
    println!("Type messages below and press Enter.");
    println!();

    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut lines = stdin.lock().lines();

    loop {
        write!(stdout, "> ").context("failed to write prompt")?;
        stdout.flush().context("failed to flush prompt")?;

        let Some(line) = lines.next() else {
            break;
        };
        let line = line.context("failed to read input line")?;
        if line.trim().is_empty() {
            continue;
        }

        let record = append_record(client, base_url, &line)?;
        println!("{}", render_append_ok(&record));
    }

    Ok(())
}

fn fetch_tail(client: &Client, base_url: &str) -> Result<TailResponse> {
    read_typed(
        client
            .get(endpoint(base_url, "/records/tail"))
            .send()
            .context("tail request failed")?,
    )
}

fn fetch_read(client: &Client, base_url: &str, seq_num: &str, limit: u32) -> Result<ReadResponse> {
    let query = [
        ("seq_num", seq_num.to_owned()),
        ("limit", limit.to_string()),
    ];
    read_typed(
        client
            .get(endpoint(base_url, "/records"))
            .query(&query)
            .send()
            .context("read request failed")?,
    )
}

fn fetch_latest(client: &Client, base_url: &str) -> Result<Option<DemoRecord>> {
    let tail = fetch_tail(client, base_url)?;
    if is_empty_tail(&tail) {
        return Ok(None);
    }

    Ok(fetch_read(client, base_url, &tail.seq_num, 1)?
        .records
        .into_iter()
        .next())
}

fn watch(
    client: &Client,
    base_url: &str,
    seq_num: Option<String>,
    limit: u32,
    interval_ms: u64,
) -> Result<()> {
    if limit == 0 {
        bail!("--limit must be greater than 0");
    }

    print_watch_banner(base_url, seq_num.as_deref());

    let mut last_printed_seq = None;
    let mut reconnect_attempt = 0u32;

    match seq_num {
        Some(start_seq) => {
            let read = fetch_read(client, base_url, &start_seq, limit)?;
            print_records(&read.records)?;
            last_printed_seq = read.records.last().map(|record| record.seq_num.clone());
        }
        None => {
            if let Some(record) = fetch_latest(client, base_url)? {
                print_record(&record)?;
                last_printed_seq = Some(record.seq_num);
            }
        }
    }

    loop {
        match poll_once(client, base_url, last_printed_seq.clone(), limit) {
            Ok(records) => {
                if reconnect_attempt > 0 {
                    let resume_from = last_printed_seq.as_deref().unwrap_or("0");
                    println!("Reconnected (resuming from seq {})", resume_from);
                    reconnect_attempt = 0;
                }
                if let Some(record) = records.last() {
                    last_printed_seq = Some(record.seq_num.clone());
                }
                print_records(&records)?;
            }
            Err(error) => {
                reconnect_attempt = reconnect_attempt.saturating_add(1);
                eprintln!("Stream error: {error:#}");
                eprintln!(
                    "Reconnecting in {}ms... (attempt {})",
                    interval_ms, reconnect_attempt
                );
            }
        }

        thread::sleep(Duration::from_millis(interval_ms));
    }
}

fn poll_once(
    client: &Client,
    base_url: &str,
    last_printed_seq: Option<String>,
    limit: u32,
) -> Result<Vec<DemoRecord>> {
    let tail = fetch_tail(client, base_url)?;
    if is_empty_tail(&tail) {
        return Ok(Vec::new());
    }

    if last_printed_seq.as_deref() == Some(tail.seq_num.as_str()) {
        return Ok(Vec::new());
    }

    let start_seq = last_printed_seq.clone().unwrap_or_else(|| "0".to_owned());
    let request_limit = if last_printed_seq.is_some() {
        limit.saturating_add(1)
    } else {
        limit
    };

    let mut records = fetch_read(client, base_url, &start_seq, request_limit)?.records;
    drop_duplicate_boundary(&mut records, last_printed_seq.as_deref());
    Ok(records)
}

fn drop_duplicate_boundary(records: &mut Vec<DemoRecord>, last_printed_seq: Option<&str>) {
    if let Some(last_seq) = last_printed_seq
        && records.first().map(|record| record.seq_num.as_str()) == Some(last_seq)
    {
        records.remove(0);
    }
}

fn print_watch_banner(base_url: &str, start_seq: Option<&str>) {
    match start_seq {
        Some(seq_num) => println!(
            "Watching {} from seq {} via {}",
            STREAM_NAME, seq_num, base_url
        ),
        None => println!("Watching {} from latest via {}", STREAM_NAME, base_url),
    }
    println!("Press Ctrl-C to stop.");
    println!();
}

fn print_records(records: &[DemoRecord]) -> Result<()> {
    for record in records {
        print_record(record)?;
    }
    Ok(())
}

fn print_record(record: &DemoRecord) -> Result<()> {
    println!("{}", render_watch_record(record)?);
    Ok(())
}

fn render_append_ok(record: &DemoRecord) -> String {
    format!(
        "OK: seq={}, timestamp={}",
        record.seq_num,
        format_ts_ms(record.ts_ms)
    )
}

fn render_watch_record(record: &DemoRecord) -> Result<String> {
    render_watch_record_at(record, current_time_ms()?)
}

fn render_watch_record_at(record: &DemoRecord, now_ms: i64) -> Result<String> {
    let age_ms = now_ms.saturating_sub(record.ts_ms);
    Ok(format!(
        "{} ({}, {}, +{}ms)",
        compact_body(&record.body),
        record.seq_num,
        format_ts_ms(record.ts_ms),
        age_ms
    ))
}

fn compact_body(body: &str) -> String {
    body.replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn format_ts_ms(ts_ms: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(ts_ms)
        .map(|dt| dt.format("%m/%d/%Y, %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| ts_ms.to_string())
}

fn current_time_ms() -> Result<i64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before unix epoch")?;
    i64::try_from(now.as_millis()).context("current time overflowed i64 milliseconds")
}

fn is_empty_tail(tail: &TailResponse) -> bool {
    tail.seq_num == "0" && tail.ts_ms == 0
}

fn read_json(response: Response) -> Result<Value> {
    let status = response.status();
    let body = response
        .text()
        .context("failed to read HTTP response body")?;
    if !status.is_success() {
        bail!("request failed with status {}: {}", status, body);
    }
    serde_json::from_str(&body)
        .map_err(|e| anyhow!(e))
        .context("invalid JSON response body")
}

fn read_typed<T>(response: Response) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let json = read_json(response)?;
    serde_json::from_value(json)
        .map_err(|e| anyhow!(e))
        .context("unexpected JSON response body")
}

#[cfg(test)]
mod tests {
    use super::{
        DemoRecord, compact_body, drop_duplicate_boundary, endpoint, render_append_ok,
        render_watch_record_at,
    };

    #[test]
    fn test_endpoint_avoids_double_slash() {
        assert_eq!(
            endpoint("http://127.0.0.1:4560/demo/", "/records"),
            "http://127.0.0.1:4560/demo/records"
        );
    }

    #[test]
    fn test_endpoint_keeps_base_without_trailing_slash() {
        assert_eq!(
            endpoint("http://127.0.0.1:4560/demo", "/records/tail"),
            "http://127.0.0.1:4560/demo/records/tail"
        );
    }

    #[test]
    fn test_drop_duplicate_boundary() {
        let mut records = vec![
            DemoRecord {
                seq_num: "10".to_owned(),
                ts_ms: 1,
                body: "old".to_owned(),
            },
            DemoRecord {
                seq_num: "20".to_owned(),
                ts_ms: 2,
                body: "new".to_owned(),
            },
        ];

        drop_duplicate_boundary(&mut records, Some("10"));

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].seq_num, "20");
    }

    #[test]
    fn test_compact_body() {
        assert_eq!(compact_body("a\nb\tc"), "a\\nb\\tc");
    }

    #[test]
    fn test_render_append_ok() {
        let record = DemoRecord {
            seq_num: "42".to_owned(),
            ts_ms: 0,
            body: "hello".to_owned(),
        };

        let rendered = render_append_ok(&record);
        assert!(rendered.contains("seq=42"));
        assert!(rendered.contains("01/01/1970"));
    }

    #[test]
    fn test_render_watch_record_at() {
        let record = DemoRecord {
            seq_num: "7".to_owned(),
            ts_ms: 1_000,
            body: "hello\nworld".to_owned(),
        };

        let rendered = render_watch_record_at(&record, 1_250).unwrap();
        assert!(rendered.contains("hello\\nworld"));
        assert!(rendered.contains("7"));
        assert!(rendered.contains("+250ms"));
    }
}
