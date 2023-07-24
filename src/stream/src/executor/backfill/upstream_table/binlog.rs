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

pub trait UpstreamBinlogOffsetRead {
    fn current_binlog_offset(&self) -> Option<String>;
}

// There is no a consistent way to get the binlog offset from different upstreams database,
// so we need to implement a specific reader for different database.
pub enum UpstreamBinlogReaderImpl {
    MYSQL(MySqlBinlogReader),
    POSTGRES(PostgresBinlogReader),
}

impl UpstreamBinlogOffsetRead for UpstreamBinlogReaderImpl {
    fn current_binlog_offset(&self) -> Option<String> {
        match self {
            UpstreamBinlogReaderImpl::MYSQL(mysql) => mysql.current_binlog_offset(),
            UpstreamBinlogReaderImpl::POSTGRES(pg) => pg.current_binlog_offset(),
            _ => unreachable!("unsupoorted upstream binlog reader"),
        }
    }
}

pub struct PostgresBinlogReader {}

pub struct MySqlBinlogReader {
    config: Option<()>,
}

impl UpstreamBinlogOffsetRead for MySqlBinlogReader {
    fn current_binlog_offset(&self) -> Option<String> {
        println!("current_binlog_offset for mysql");
        todo!()
    }
}

impl UpstreamBinlogOffsetRead for PostgresBinlogReader {
    fn current_binlog_offset(&self) -> Option<String> {
        println!("current_binlog_offset for postgres");
        todo!()
    }
}

// unit test for binlog readers
#[cfg(test)]
mod tests {

    // tokio unit test
    #[tokio::test]
    async fn test_mysql_binlog_reader() {}
}
