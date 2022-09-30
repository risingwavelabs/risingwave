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
use std::result::Result;
use std::sync::Arc;

use futures::Stream;
use tokio::net::TcpListener;

use crate::pg_field_descriptor::PgFieldDescriptor;
use crate::pg_protocol::PgProtocol;
use crate::pg_response::{PgResponse, RowSetResult};

pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;
pub type SessionId = (i32, i32);
/// The interface for a database system behind pgwire protocol.
/// We can mock it for testing purpose.
pub trait SessionManager<VS>: Send + Sync + 'static
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    type Session: Session<VS>;

    fn connect(&self, database: &str, user_name: &str) -> Result<Arc<Self::Session>, BoxedError>;

    fn cancel_queries_in_session(&self, session_id: SessionId);
}

/// A psql connection. Each connection binds with a database. Switching database will need to
/// recreate another connection.
///
/// format:
/// false: TEXT
/// true: BINARY
#[async_trait::async_trait]
pub trait Session<VS>: Send + Sync
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    async fn run_statement(
        self: Arc<Self>,
        sql: &str,
        format: bool,
    ) -> Result<PgResponse<VS>, BoxedError>;
    async fn infer_return_type(
        self: Arc<Self>,
        sql: &str,
    ) -> Result<Vec<PgFieldDescriptor>, BoxedError>;
    fn user_authenticator(&self) -> &UserAuthenticator;

    fn id(&self) -> SessionId;
}

#[derive(Debug, Clone)]
pub enum UserAuthenticator {
    // No need to authenticate.
    None,
    // raw password in clear-text form.
    ClearText(Vec<u8>),
    // password encrypted with random salt.
    MD5WithSalt {
        encrypted_password: Vec<u8>,
        salt: [u8; 4],
    },
}

impl UserAuthenticator {
    pub fn authenticate(&self, password: &[u8]) -> bool {
        match self {
            UserAuthenticator::None => true,
            UserAuthenticator::ClearText(text) => password == text,
            UserAuthenticator::MD5WithSalt {
                encrypted_password, ..
            } => encrypted_password == password,
        }
    }
}

/// Binds a Tcp listener at `addr`. Spawn a coroutine to serve every new connection.
pub async fn pg_serve<VS>(addr: &str, session_mgr: Arc<impl SessionManager<VS>>) -> io::Result<()>
where
    VS: Stream<Item = RowSetResult> + Unpin + Send,
{
    let listener = TcpListener::bind(addr).await.unwrap();
    // accept connections and process them, spawning a new thread for each one
    tracing::info!("Server Listening at {}", addr);
    loop {
        let session_mgr = session_mgr.clone();
        let conn_ret = listener.accept().await;
        match conn_ret {
            Ok((stream, peer_addr)) => {
                tracing::info!("New connection: {}", peer_addr);
                stream.set_nodelay(true)?;
                tokio::spawn(async move {
                    // connection succeeded
                    let mut pg_proto = PgProtocol::new(stream, session_mgr);
                    while !pg_proto.process().await {}
                    tracing::info!("Connection {} closed", peer_addr);
                });
            }

            Err(e) => {
                tracing::error!("Connection failure: {}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::stream::BoxStream;
    use futures::StreamExt;
    use tokio_postgres::types::*;
    use tokio_postgres::NoTls;

    use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
    use crate::pg_response::{PgResponse, RowSetResult, StatementType};
    use crate::pg_server::{pg_serve, Session, SessionId, SessionManager, UserAuthenticator};
    use crate::types::Row;

    struct MockSessionManager {}

    impl SessionManager<BoxStream<'static, RowSetResult>> for MockSessionManager {
        type Session = MockSession;

        fn connect(
            &self,
            _database: &str,
            _user_name: &str,
        ) -> Result<Arc<Self::Session>, Box<dyn Error + Send + Sync>> {
            Ok(Arc::new(MockSession {}))
        }

        fn cancel_queries_in_session(&self, _session_id: SessionId) {
            todo!()
        }
    }

    struct MockSession {}

    #[async_trait::async_trait]
    impl Session<BoxStream<'static, RowSetResult>> for MockSession {
        async fn run_statement(
            self: Arc<Self>,
            sql: &str,
            _format: bool,
        ) -> Result<PgResponse<BoxStream<'static, RowSetResult>>, Box<dyn Error + Send + Sync>>
        {
            // split a statement and trim \' around the input param to construct result.
            // Ex:
            //    SELECT 'a','b' -> result: a , b
            let res: Vec<Option<Bytes>> = sql
                .split(&[' ', ',', ';'])
                .skip(1)
                .map(|x| {
                    Some(
                        x.trim_start_matches('\'')
                            .trim_end_matches('\'')
                            .to_string()
                            .into(),
                    )
                })
                .collect();
            let len = res.len();

            Ok(PgResponse::new_for_stream(
                StatementType::SELECT,
                Some(1),
                futures::stream::iter(vec![Ok(vec![Row::new(res)])]).boxed(),
                // NOTE: Extended mode don't need.
                vec![PgFieldDescriptor::new("".to_string(), TypeOid::Varchar); len],
            ))
        }

        fn user_authenticator(&self) -> &UserAuthenticator {
            &UserAuthenticator::None
        }

        async fn infer_return_type(
            self: Arc<Self>,
            sql: &str,
        ) -> Result<Vec<PgFieldDescriptor>, super::BoxedError> {
            let count = sql.split(&[' ', ',', ';']).skip(1).count();
            Ok(vec![
                PgFieldDescriptor::new("".to_string(), TypeOid::Varchar,);
                count
            ])
        }

        fn id(&self) -> SessionId {
            (0, 0)
        }
    }

    // test_psql_extended_mode_explicit_simple
    // constrain:
    // - Only support simple SELECT statement.
    // - Must provide all type description of the generic types.
    // - Input description(params description) should include all the generic params description we
    //   need.
    #[tokio::test]
    async fn test_psql_extended_mode_explicit_simple() {
        let session_mgr = Arc::new(MockSessionManager {});
        tokio::spawn(async move { pg_serve("127.0.0.1:10000", session_mgr).await });
        // wait for server to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

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

        // explicit parameter (test pre_statement)
        {
            let statement = client
                .prepare_typed("SELECT $1;", &[Type::VARCHAR])
                .await
                .unwrap();

            let rows = client.query(&statement, &[&"AA"]).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "AA");

            let rows = client.query(&statement, &[&"BB"]).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "BB");
        }
        // explicit parameter (test portal)
        {
            let transaction = client.transaction().await.unwrap();
            let statement = transaction
                .prepare_typed("SELECT $1;", &[Type::VARCHAR])
                .await
                .unwrap();
            let portal1 = transaction.bind(&statement, &[&"AA"]).await.unwrap();
            let portal2 = transaction.bind(&statement, &[&"BB"]).await.unwrap();
            let rows = transaction.query_portal(&portal1, 0).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "AA");
            let rows = transaction.query_portal(&portal2, 0).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "BB");
            transaction.rollback().await.unwrap();
        }
        // mix parameter
        {
            let statement = client
                .prepare_typed("SELECT $1,$2;", &[Type::VARCHAR, Type::VARCHAR])
                .await
                .unwrap();
            let rows = client.query(&statement, &[&"AA", &"BB"]).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "AA");
            let value: &str = rows[0].get(1);
            assert_eq!(value, "BB");

            let statement = client
                .prepare_typed("SELECT $1,$1;", &[Type::VARCHAR])
                .await
                .unwrap();
            let rows = client.query(&statement, &[&"AA"]).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "AA");
            let value: &str = rows[0].get(1);
            assert_eq!(value, "AA");

            let statement = client
                .prepare_typed(
                    "SELECT $2,$3,$1,$3,$2;",
                    &[Type::VARCHAR, Type::VARCHAR, Type::VARCHAR],
                )
                .await
                .unwrap();
            let rows = client
                .query(&statement, &[&"AA", &"BB", &"CC"])
                .await
                .unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "BB");
            let value: &str = rows[0].get(1);
            assert_eq!(value, "CC");
            let value: &str = rows[0].get(2);
            assert_eq!(value, "AA");
            let value: &str = rows[0].get(3);
            assert_eq!(value, "CC");
            let value: &str = rows[0].get(4);
            assert_eq!(value, "BB");

            let statement = client
                .prepare_typed(
                    "SELECT $3,$1;",
                    &[Type::VARCHAR, Type::VARCHAR, Type::VARCHAR],
                )
                .await
                .unwrap();
            let rows = client
                .query(&statement, &[&"AA", &"BB", &"CC"])
                .await
                .unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "CC");
            let value: &str = rows[0].get(1);
            assert_eq!(value, "AA");

            let statement = client
                .prepare_typed(
                    "SELECT $2,$1;",
                    &[Type::VARCHAR, Type::VARCHAR, Type::VARCHAR],
                )
                .await
                .unwrap();
            let rows = client
                .query(&statement, &[&"AA", &"BB", &"CC"])
                .await
                .unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "BB");
            let value: &str = rows[0].get(1);
            assert_eq!(value, "AA");
        }
        // no params
        {
            let rows = client.query("SELECT 'AA','BB';", &[]).await.unwrap();
            let value: &str = rows[0].get(0);
            assert_eq!(value, "AA");
            let value: &str = rows[0].get(1);
            assert_eq!(value, "BB");
        }
    }
}
