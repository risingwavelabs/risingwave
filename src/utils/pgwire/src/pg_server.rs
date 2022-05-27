// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::io::ErrorKind;
use std::result::Result;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};

use crate::pg_protocol::PgProtocol;
use crate::pg_response::PgResponse;

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager: Send + Sync + 'static {
    type Session: Session;

    fn connect(&self, database: &str) -> Result<Arc<Self::Session>, BoxedError>;
}

/// A psql connection. Each connection binds with a database. Switching database will need to
/// recreate another connection.
#[async_trait::async_trait]
pub trait Session: Send + Sync {
    async fn run_statement(self: Arc<Self>, sql: &str) -> Result<PgResponse, BoxedError>;
}

/// Binds a Tcp listener at `addr`. Spawn a coroutine to serve every new connection.
pub async fn pg_serve(addr: &str, session_mgr: Arc<impl SessionManager>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await.unwrap();
    // accept connections and process them, spawning a new thread for each one
    tracing::info!("Server Listening at {}", addr);
    loop {
        let session_mgr = session_mgr.clone();
        let conn_ret = listener.accept().await;
        match conn_ret {
            Ok((stream, peer_addr)) => {
                tracing::info!("New connection: {}", peer_addr);
                tokio::spawn(async move {
                    // connection succeeded
                    pg_serve_conn(stream, session_mgr).await;
                    tracing::info!("Connection {} closed", peer_addr);
                });
            }

            Err(e) => {
                tracing::error!("Connection failure: {}", e);
            }
        }
    }
}

async fn pg_serve_conn(socket: TcpStream, session_mgr: Arc<impl SessionManager>) {
    let mut pg_proto = PgProtocol::new(socket, session_mgr);
    let mut unnamed_query_string = bytes::Bytes::new();
    loop {
        let terminate = pg_proto.process(&mut unnamed_query_string).await;
        match terminate {
            Ok(is_ter) => {
                if is_ter {
                    break;
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                // Execution error should not break current connection.
                // For unexpected eof, just break and not print to log.
                tracing::error!("Error {:?}!", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::intrinsics::transmute;
    use std::sync::Arc;

    use tokio_postgres::types::*;
    use tokio_postgres::{NoTls, Transaction};

    use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
    use crate::pg_response::{PgResponse, StatementType};
    use crate::pg_server::{pg_serve, Session, SessionManager};
    use crate::types::Row;

    struct MockSessionManager {}

    impl SessionManager for MockSessionManager {
        type Session = MockSession;

        fn connect(
            &self,
            _database: &str,
        ) -> Result<Arc<Self::Session>, Box<dyn Error + Send + Sync>> {
            Ok(Arc::new(MockSession {}))
        }
    }

    struct MockSession {}

    #[async_trait::async_trait]
    impl Session for MockSession {
        async fn run_statement(
            self: Arc<Self>,
            _sql: &str,
        ) -> Result<PgResponse, Box<dyn Error + Send + Sync>> {
            Ok(PgResponse::new(
                StatementType::SELECT,
                1,
                vec![Row::new(vec![Some("Hello, World".to_owned())])],
                vec![PgFieldDescriptor::new(
                    "VARCHAR".to_owned(),
                    TypeOid::Varchar,
                )],
            ))
        }
    }

    #[tokio::test]
    /// The test below is copied from tokio-postgres doc.
    async fn test_psql_extended_mode_connect() {
        let session_mgr = Arc::new(MockSessionManager {});
        tokio::spawn(async move { pg_serve("127.0.0.1:10000", session_mgr).await });

        // Connect to the database.
        let (client, connection) = tokio_postgres::connect("host=localhost port=10000", NoTls)
            .await
            .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Now we can execute a simple statement that just returns its parameter.
        let rows = client.query("SELECT 'Hello, World'", &[]).await.unwrap();
        // FIXME: Enable this after handle prepared statement.
        // let rows = client
        //     .query("SELECT $1::TEXT", &[&"hello world"])
        //     .await
        //     .unwrap();

        let value: &str = rows[0].get(0);
        assert_eq!(value, "Hello, World");
    }

    // test_psql_extended_mode_explicit
    // constrain:
    // - explict parameter type (server needn't to infer parameter type)
    // - SELECT
    // - (option) INSERT
    #[tokio::test]
    async fn test_psql_extended_mode_exlicit() {
        let session_mgr = Arc::new(MockSessionManager {});
        tokio::spawn(async move { pg_serve("127.0.0.1:10000", session_mgr).await });

        // Connect to the database.
        let (mut client, connection) = tokio_postgres::connect("host=localhost port=10000", NoTls)
            .await
            .unwrap();

        // The connection object performs the actual communication with the database,
        // so spawn it off to run on its own.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // non parameter (test pre_statement)
        {
            let statement1 = client.prepare("SELECT 1,true,'hello world'").await.unwrap();
            let statement2 = client
                .prepare("SELECT 2,false,'hello world'")
                .await
                .unwrap();
            let rows = client.query(&statement1, &[]).await.unwrap();
            let value: i32 = rows[0].get(0);
            assert_eq!(value, 1);
            let value: bool = rows[0].get(1);
            assert_eq!(value, true);
            let value: &str = rows[0].get(2);
            assert_eq!(value, "hello world");
            let rows = client.query(&statement2, &[]).await.unwrap();
            let value: i32 = rows[0].get(0);
            assert_eq!(value, 2);
            let value: bool = rows[0].get(1);
            assert_eq!(value, false);
            let value: &str = rows[0].get(2);
            assert_eq!(value, "hello world");
        }
        // explicit parameter (test pre_statement)
        {
            let statement = client
                .prepare_typed("SELECT $1", &[Type::TEXT])
                .await
                .unwrap();
            let rows = client.query(&statement, &[&"Hello, World"]).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "Hello, World");
            let rows = client
                .query(&statement, &[&"Statement is still useful"])
                .await
                .unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "Statement is still useful");
        }
        // explict parameter (test portal)
        {
            let transaction = client.transaction().await.unwrap();
            let statement = transaction
                .prepare_typed("SELECT $1", &[Type::TEXT])
                .await
                .unwrap();
            let portal1 = transaction
                .bind(&statement, &[&"Hello,World"])
                .await
                .unwrap();
            let portal2 = transaction
                .bind(&statement, &[&"Portal is still useful"])
                .await
                .unwrap();
            let rows = transaction.query_portal(&portal1, 2).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "Hello, World");
            let rows = transaction.query_portal(&portal2, 2).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "Portal is still useful");
            transaction.rollback().await.unwrap();
        }
    }
}
