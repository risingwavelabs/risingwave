// Copyright 2023 RisingWave Labs
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

//! This is a test program by libpq.
//! We can compare the result between RisingWave and Postgres easily using the program.
use clap::Parser;

#[derive(clap::Parser, Clone, Debug)]
struct TestOptions {
    /// The database server host.
    #[clap(long, default_value = "localhost")]
    host: String,

    /// The database server port.
    #[clap(short, long, default_value = "4566")]
    port: u16,

    /// The database name to connect.
    #[clap(short, long, default_value = "dev")]
    db: String,

    /// The database username.
    #[clap(short, long, default_value = "root")]
    user: String,

    /// The database password.
    #[clap(short = 'w', long, default_value = "")]
    pass: String,
}

fn main() {
    let TestOptions {
        host,
        port,
        db,
        user,
        pass,
    } = TestOptions::parse();
    let conn =
        libpq::Connection::new(&format!("postgresql://{user}:{pass}@{host}:{port}/{db}")).unwrap();

    // Test the unnamed empty prepare query in extended query mode.
    {
        let prep_res = conn.prepare(None, "", &[]);
        assert_eq!(prep_res.status(), libpq::Status::CommandOk);
        let desc_res = conn.describe_prepared(None);
        assert_eq!(desc_res.status(), libpq::Status::CommandOk);
        let exec_res = conn.exec_prepared(None, &[], &[], libpq::Format::Text);
        assert_eq!(exec_res.status(), libpq::Status::EmptyQuery);
    }
    // Test the named empty prepare query in extended query mode.
    {
        let prep_res = conn.prepare(Some("name1"), "", &[]);
        assert_eq!(prep_res.status(), libpq::Status::CommandOk);
        let desc_res = conn.describe_prepared(Some("name1"));
        assert_eq!(desc_res.status(), libpq::Status::CommandOk);
        let exec_res = conn.exec_prepared(Some("name1"), &[], &[], libpq::Format::Text);
        assert_eq!(exec_res.status(), libpq::Status::EmptyQuery);
    }
}
