/// Message type in psql connection.
pub enum PgMessage {
    Ssl(SslMessage),
    Startup(StartupMessage),
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
