use std::sync::Arc;

use log::{error, info};
use risingwave_common::error::Result;
use tokio::net::{TcpListener, TcpStream};

use super::pg_protocol::PgProtocol;
use super::pg_result::PgResult;

/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager: Send + Sync {
    fn connect(&self) -> Box<dyn Session>;
}

/// A psql connection.
#[async_trait::async_trait]
pub trait Session: Send + Sync {
    async fn run_statement(&self, sql: &str) -> PgResult;
}

/// Binds a Tcp listener at [`addr`]. Spawn a coroutine to serve every new connection.
pub async fn pg_serve(addr: &str, session_mgr: Arc<dyn SessionManager>) -> Result<()> {
    let listener = TcpListener::bind(addr).await.unwrap();
    // accept connections and process them, spawning a new thread for each one
    info!("Starting server at {}", addr);
    loop {
        let session_mgr = session_mgr.clone();
        let conn_ret = listener.accept().await;
        match conn_ret {
            Ok((stream, peer_addr)) => {
                info!("New connection: {}", peer_addr);
                tokio::spawn(async move {
                    // connection succeeded
                    pg_serve_conn(stream, session_mgr).await;
                });
            }

            Err(e) => {
                error!("Connection failure: {}", e);
            }
        }
    }
}

async fn pg_serve_conn(socket: TcpStream, session_mgr: Arc<dyn SessionManager>) {
    let mut pg_proto = PgProtocol::new(socket, session_mgr);
    loop {
        let terminate = pg_proto.process().await.unwrap();
        if terminate {
            println!("Connection closed!");
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::Row;
    use risingwave_common::types::ScalarImpl;
    use tokio_postgres::{NoTls, SimpleQueryMessage};

    use super::{Session, SessionManager};
    use crate::pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
    use crate::pgwire::pg_result::{PgResult, StatementType};
    use crate::pgwire::pg_server::pg_serve;

    struct TestSessionManager {}

    impl SessionManager for TestSessionManager {
        fn connect(&self) -> Box<dyn super::Session> {
            Box::new(TestSession {})
        }
    }

    struct TestSession {}

    #[async_trait::async_trait]
    impl Session for TestSession {
        async fn run_statement(&self, sql: &str) -> PgResult {
            // Returns a single-column single-row result, containing the sql string.
            PgResult::new(
                StatementType::SELECT,
                1,
                vec![Row::new(vec![Some(ScalarImpl::Utf8(sql.to_string()))])],
                vec![PgFieldDescriptor::new("sql".to_string(), TypeOid::Varchar)],
            )
        }
    }

    #[tokio::test]
    /// Test the psql connection establish of PG server.
    async fn test_connection() {
        tokio::spawn(
            async move { pg_serve("127.0.0.1:45661", Arc::new(TestSessionManager {})).await },
        );
        // Connect to the database.
        let (client, connection) = tokio_postgres::connect("host=localhost port=45661", NoTls)
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
                    assert_eq!(row_inner.get(0), Some("SELECT * from t;"));
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
