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
    sql: String,
}

impl QueryMessage {
    pub fn new(buf: &[u8]) -> Self {
        match std::str::from_utf8(buf) {
            Ok(v) => Self { sql: v.to_string() },
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        }
    }

    pub fn get_sql(&self) -> &str {
        &self.sql
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
