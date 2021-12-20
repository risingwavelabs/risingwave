use clap::Parser;
use pgwire::pg_server::PgServer;
#[derive(Parser)]
struct Opts {
    // The custom log4rs config file.
    #[clap(long, default_value = "config/log4rs.yaml")]
    log4rs_config: String,

    #[clap(long, default_value = "127.0.0.1:4567")]
    host: String,
}

#[cfg(not(tarpaulin_include))]
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let opts: Opts = Opts::parse();
    log4rs::init_file(&opts.log4rs_config, Default::default()).unwrap();

    PgServer::serve(&opts.host).await;
}

#[cfg(test)]
mod tests {
    use pgwire::pg_server::PgServer;
    use tokio_postgres::NoTls;

    #[tokio::test]
    /// Test the psql connection establish of PG server.
    async fn test_connection() {
        tokio::spawn(async move { PgServer::serve("127.0.0.1:4567").await });
        // Connect to the database.
        let (client, connection) = tokio_postgres::connect("host=localhost port=4567", NoTls)
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
        client.simple_query("SELECT 1").await.unwrap();
    }
}
