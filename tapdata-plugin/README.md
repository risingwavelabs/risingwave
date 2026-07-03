# Tapdata PDK Connector for RisingWave

A [Tapdata PDK](https://github.com/tapdata/tapdata) connector that lets Tapdata write data to
RisingWave. It uses the PostgreSQL JDBC driver internally, since RisingWave is PostgreSQL
wire-protocol compatible.

## What it does

- Replicates tables from any Tapdata source (e.g. PostgreSQL) into RisingWave
- Supports batch snapshot sync and CDC (change-data-capture) streaming
- Handles RisingWave SQL dialect differences from PostgreSQL automatically
  (strips `varchar(N)` lengths, `numeric(p,s)` precision, etc.)

## Prerequisites

| Component  | Version tested | Notes |
|------------|---------------|-------|
| Java       | 11+           | Maven build only |
| Maven      | 3.8+          |       |
| Docker     | any           | Runs Tapdata |
| RisingWave | any           | Must be reachable from Docker container |
| PostgreSQL | 13+           | Example source; any Tapdata source works |

## Build

```bash
cd tapdata-plugin
mvn package -DskipTests
# Produces: target/risingwave-connector-1.0-SNAPSHOT.jar
```

## Deploy to Tapdata

### 1. Start Tapdata (if not already running)

```bash
docker run -d --name tapdata \
  --add-host=host.docker.internal:host-gateway \
  -p 3030:3030 \
  ghcr.io/tapdata/tapdata:latest
```

Wait ~60 seconds for startup, then open http://localhost:3030 to confirm it's up.

### 2. Register the connector

```bash
# Copy JAR into the container
docker cp target/risingwave-connector-1.0-SNAPSHOT.jar tapdata:/tmp/rw-connector.jar

# Register via pdk-deploy
docker exec tapdata java -jar /tapdata/apps/lib/pdk-deploy.jar register \
  -t http://localhost:3030 \
  -a <your-access-code> \
  /tmp/rw-connector.jar
```

Your access code is shown in the Tapdata UI under **Settings → Access Code**, or you can
find it via the API:

```bash
curl -s http://localhost:3030/api/users/generatetoken \
  -X POST -H "Content-Type: application/json" \
  -d '{"accesscode":"<your-access-code>"}'
```

### 3. Restart Tapdata to load the new connector

```bash
docker restart tapdata
```

Wait for it to come back up (~40 seconds).

## Run the end-to-end demo (PostgreSQL → RisingWave)

The `setup_pipeline.py` script creates a full pipeline using the TapFlow Python SDK.

### Prerequisites for the demo

1. **PostgreSQL** running locally on port 5432, database `tapdata_demo` with a table `orders`,
   WAL level set to logical, and a publication created:

   ```sql
   ALTER SYSTEM SET wal_level = logical;
   -- restart PostgreSQL --
   CREATE DATABASE tapdata_demo;
   \c tapdata_demo
   CREATE TABLE orders (
     id serial PRIMARY KEY,
     customer_name varchar(100) NOT NULL,
     product varchar(100) NOT NULL,
     quantity int NOT NULL,
     price numeric(10,2) NOT NULL,
     status varchar(20) DEFAULT 'pending',
     created_at timestamp DEFAULT now()
   );
   INSERT INTO orders (customer_name, product, quantity, price) VALUES
     ('Alice', 'Laptop', 1, 999.99),
     ('Bob',   'Phone',  2, 499.99);
   CREATE PUBLICATION tapdata_pub FOR TABLE orders;
   ```

2. **RisingWave** running locally on port 4566 (e.g. via `./risedev d` in the RisingWave repo).

3. **TapFlow SDK** installed:

   ```bash
   python3 -m venv /tmp/tapflow_env
   /tmp/tapflow_env/bin/pip install tapflow
   ```

### Run the pipeline

```bash
/tmp/tapflow_env/bin/python3 setup_pipeline.py
```

The script:
1. Logs into Tapdata
2. Creates a PostgreSQL source connection pointing at `host.docker.internal:5432/tapdata_demo`
3. Creates a RisingWave target connection pointing at `host.docker.internal:4566/dev`
4. Creates and starts the replication pipeline `pg_to_risingwave_native`

### Verify

```bash
# Check rows in RisingWave
psql -h localhost -p 4566 -U root -d dev -c "SELECT * FROM orders ORDER BY id;"
```

### Test CDC

Insert a new row in PostgreSQL and watch it appear in RisingWave within ~10 seconds:

```bash
psql -h localhost -p 5432 -U <user> -d tapdata_demo \
  -c "INSERT INTO orders (customer_name, product, quantity, price) VALUES ('CDC Test', 'Widget', 1, 9.99);"

# A few seconds later:
psql -h localhost -p 4566 -U root -d dev \
  -c "SELECT * FROM orders ORDER BY id DESC LIMIT 1;"
```

## Connection parameters

| Field    | Default | Description |
|----------|---------|-------------|
| host     | —       | RisingWave hostname or IP |
| port     | 4566    | RisingWave port |
| database | dev     | Database name |
| schema   | public  | Target schema |
| user     | root    | Database user |
| password | (empty) | Database password |
| ingestEndpoint | ws://host:4560 | Full WebSocket ingest endpoint base, including `ws://` or `wss://` |
| webhookSecret | (empty) | Shared secret used to sign the initial WebSocket auth frame |

## Known limitations

- RisingWave does not support `ALTER TABLE` for adding/dropping columns, so schema evolution
  requires dropping and recreating the table.
- `ON CONFLICT DO UPDATE` (upsert) requires the target table to have a primary key.
- Array and composite PostgreSQL types are not mapped; they fall back to `text`.
