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

pub struct MySqlBinlogReader {}

impl UpstreamBinlogOffsetRead for MySqlBinlogReader {
    fn current_binlog_offset(&self) -> Option<String> {
        todo!()
    }
}

impl UpstreamBinlogOffsetRead for PostgresBinlogReader {
    fn current_binlog_offset(&self) -> Option<String> {
        todo!()
    }
}
