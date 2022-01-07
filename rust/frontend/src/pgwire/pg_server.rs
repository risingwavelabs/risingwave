use log::info;
use tokio::net::TcpListener;

use crate::pgwire::pg_server_conn::PgServerConn;

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

#[cfg(test)]
mod tests {
    use tokio_postgres::{NoTls, SimpleQueryMessage};

    use crate::pgwire::database::parse;
    use crate::pgwire::pg_server::PgServer;

    #[tokio::test]
    /// Test the psql connection establish of PG server.
    async fn test_connection() {
        tokio::spawn(async move { PgServer::serve("127.0.0.1:4566").await });
        // Connect to the database.
        let (client, connection) = tokio_postgres::connect("host=localhost port=4566", NoTls)
            .await
            .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Now we can execute a simple statement that just returns its AST.
        let query = "SELECT * from t;";
        let ret = client.simple_query(query).await.unwrap();
        assert_eq!(ret.len(), 2);
        for (idx, row) in ret.iter().enumerate() {
            if idx == 0 {
                if let SimpleQueryMessage::Row(row_inner) = row {
                    assert_eq!(
                        row_inner.get(0),
                        Some(&format!("{:?}", parse(query).unwrap())[..])
                    );
                } else {
                    panic!("The first message should be row values")
                }
            } else if idx == 1 {
                if let SimpleQueryMessage::CommandComplete(row_inner) = row {
                    assert_eq!(*row_inner, 1);
                } else {
                    panic!("The last message should be command complete")
                }
            }
        }
    }
}
