use bytes::Bytes;

/// Message type in psql connection.
pub enum PgMessage {
    Ssl(SslMessage),
    Startup(StartupMessage),
    Query(QueryMessage),
    Terminate(TerminateMessage),
}

pub struct SslMessage {}

impl SslMessage {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SslMessage {
    fn default() -> Self {
        Self::new()
    }
}

pub struct StartupMessage {}

impl StartupMessage {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for StartupMessage {
    fn default() -> Self {
        Self::new()
    }
}

/// Query message contains the string sql.
pub struct QueryMessage {
    sql: Bytes,
}

impl QueryMessage {
    pub fn new(buf: Vec<u8>) -> Self {
        Self {
            sql: Bytes::from(buf),
        }
    }

    pub fn get_sql(&self) -> &str {
        // Why there is a \0..
        match std::str::from_utf8(&self.sql[..]) {
            Ok(v) => v.trim_end_matches('\0'),
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        }
    }
}

pub struct TerminateMessage {}

impl TerminateMessage {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for TerminateMessage {
    fn default() -> Self {
        Self::new()
    }
}
