use log::info;
use tokio::net::TcpListener;

use crate::pg_server_conn::PgServerConn;

pub struct PgServer {}

impl PgServer {
    /// Binds a Tcp listener at [`addr`]. Spawn a thread to serve every new connection.
    pub async fn serve(addr: &str) {
        let listener = TcpListener::bind(addr).await.unwrap();
        // accept connections and process them, spawning a new thread for each one
        info!("Starting server at {}", addr);
        loop {
            let conn_ret = listener.accept().await;
            match conn_ret {
                Ok((stream, _)) => {
                    info!("New connection: {}", stream.peer_addr().unwrap());
                    let mut pg_conn = PgServerConn::new(stream);
                    tokio::spawn(async move {
                        // connection succeeded
                        pg_conn.serve().await
                    });
                }

                Err(e) => {
                    info!("Error: {}", e);
                }
            }
        }
    }
}
