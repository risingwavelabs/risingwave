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

use std::time::Instant;

use clap::{Parser, ValueEnum};
use nexmark::event::{Event, EventType};
use nexmark::EventGenerator;

/// Nexmark event generator.
#[derive(Debug, Parser)]
pub struct Args {
    /// The type of events to generate.
    #[clap(short, long = "type", value_enum, default_value = "all")]
    type_: Type,

    /// The number of events to generate.
    /// If not specified, generate events forever.
    #[clap(short, long)]
    number: Option<usize>,

    /// Print format.
    #[clap(long, value_enum, default_value = "json")]
    format: Format,

    /// Generate all events immediately.
    #[clap(long)]
    no_wait: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Type {
    All,
    Person,
    Auction,
    Bid,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Format {
    /// JSON format.
    Json,
    /// Rust debug format.
    Rust,
}

fn main() {
    let opts = Args::parse();
    let number = opts.number.unwrap_or(usize::MAX);

    let iter = EventGenerator::default();
    let mut iter = match opts.type_ {
        Type::All => iter,
        Type::Person => iter.with_type_filter(EventType::Person),
        Type::Auction => iter.with_type_filter(EventType::Auction),
        Type::Bid => iter.with_type_filter(EventType::Bid),
    };
    let start_time = Instant::now();
    let mut i = 0;
    while let Some(event) = iter.next() {
        if !opts.no_wait {
            // sleep until the timestamp of the event
            if let Some(t) = (start_time + iter.elapsed()).checked_duration_since(Instant::now()) {
                std::thread::sleep(t);
            }
        }
        match opts.format {
            Format::Json => println!("{}", serde_json::to_string(&event).unwrap()),
            Format::Rust => match &event {
                Event::Person(e) => println!("{e:?}"),
                Event::Auction(e) => println!("{e:?}"),
                Event::Bid(e) => println!("{e:?}"),
            },
        }
        i += 1;
        if i >= number {
            break;
        }
    }
}
