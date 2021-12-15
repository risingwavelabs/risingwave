use tokio::net::TcpStream;

use crate::pg_protocol::PgProtocol;

/// Each psql connection belongs to one [`PgServerConn`].
pub struct PgServerConn {
    pg_proto: PgProtocol,
}

impl PgServerConn {
    pub fn new(socket: TcpStream) -> Self {
        Self {
            pg_proto: PgProtocol::new(socket),
        }
    }

    pub async fn serve(&mut self) {
        loop {
            let terminate = self.pg_proto.process().await.unwrap();
            if terminate {
                println!("Connection closed!");
                break;
            }
        }
    }
}
