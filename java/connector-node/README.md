# RisingWave Connector Node

The RisingWave Connector Node is a connector service that bundles customizable external sinks, allowing you to easily connect to various data sources and sinks. It acts as a bridge between RisingWave and the external systems, enabling you to stream data bidirectionally between them.

## Up and running

To build the Connector Node, you will need to have Maven and Python 3 installed on your system. On Ubuntu, you can install these dependencies using the package manager:

```
sudo apt-get update
sudo apt-get install maven python3
```
To build the Connector Node, run the following command from the `risingwave/java` directory:

```
mvn clean package
```
If you meet problem, you can try the following to skip the unit test:

```
mvn clean package -DskipTests=true
```

To disable building the rust library, you can try the following:
```
mvn clean package -Dno-build-rust
```

This will create a `.tar.gz` file with the Connector Node and all its dependencies in the `risingwave/java/connector-node/assembly/target` directory. To run the Connector Node, execute the following command:

```
# unpack the tar file, the file name might vary depending on the version
cd java/connector-node/assembly/target && tar xvf risingwave-connector-1.0.0.tar.gz 
# launch connector node service
java -classpath "./libs/*" com.risingwave.connector.ConnectorService
```

Currently, during the Maven build process, all the required shared libraries are built and added to the JAR file. These shared libraries can then be loaded directly from the Java code.

Here are some tips to follow:

If you encounter an error stating that the program cannot access the library, it may be due to merging new features or outdated documentation. In such cases, you will need to manually build the corresponding Java shared library file.

After building the shared library file, move it into the `java/connector-node/assembly/target directory`, make sure to specify the shared library path using the `-Djava.library.path=java/connector-node/assembly/target` flag. This tells Java where to find the required shared library files.

By following these steps, you should be able to resolve any issues related to accessing the shared libraries in your Java program.

## Docker image
Alternatively, to build and run the Docker image, run the following command from the project's root directory:

```
docker build -t connector-node .
```

This will build the Docker image and tag it as connector-node.

To run the Connector Node in a Docker container, use the following command:

```
# The default listening port is 50051
docker run -it --rm -p 50051:50051 connector-node
```

## Integration test

To run the integration test, make sure you have Python 3 and Virtualenv installed. Additionally, you need to install PostgreSQL because sinking to PG is part of the test.

Navigate to the `python-client` directory and run the following command:

```
bash build-venv.sh
bash gen-stub.sh
python3 integration_tests.py
```

Or you can use conda and install the necessary package `grpcio grpcio-tools psycopg2 psycopg2-binary`. 

The connector service is the server and Python integration test is a client, which will send gRPC request and get response from the connector server. So when running integration_tests, remember to launch the connector service in advance. You can get the gRPC response and check messages or errors in client part. And check the detailed exception information on server side.

### JDBC test

We have integration tests that involve the use of several sinks, including file sink, jdbc sink, iceberg sink, and deltalake sink. If you wish to run these tests locally, you will need to configure both MinIO and PostgreSQL. 
Downloading and launching MinIO is a straightforward process. For PostgreSQL, I recommend launching it using Docker. When setting up PostgreSQL, please ensure that the values for `POSTGRES_PASSWORD`, `POSTGRES_DB`, and `POSTGRES_USER` match the corresponding settings in the `integration_tests.py` file.

```shell
# create postgress in docker
docker run --name my-postgres -e POSTGRES_PASSWORD=connector -e POSTGRES_DB=test -e POSTGRES_USER=test -d -p 5432:5432 postgres
# connect postgress
psql -h localhost -p 5432 -U test -d postgres
```
Also remember to create the database and tables in postgresql

```sql
# create table
CREATE TABLE test (id serial PRIMARY KEY, name VARCHAR (50) NOT NULL);
```

If you want to create a Sink to this pg instance, please check details at [here](https://www.risingwave.dev/docs/current/sink-to-postgres/). About how to launch rw in kubernetes, please check [here](https://github.com/risingwavelabs/risingwave-operator/blob/main/README.md).

By maintaining consistency between these configurations, you can ensure a smooth execution of the integration tests. Please check more details in `connector-node-integration.yml`.

Also, if you change the file in `src/java_binding`, remember to use `cargo fmt` and `cargo sort` to format the `Cargo.toml` file to pass the github ci check.

## Connect with RisingWave

Connector node is optional to running a RisingWave cluster. It is only on creating external sources and sinks that the connector node service will be automatically called.

Currently, the following external sources and sinks depends on the connector node:

### Sinks
- JDBC
- Iceberg

### Sources
- CDC

Creating a sink with external connectors above will check for the connector node service. If the service is not running, the creation will fail. 

```sql
CREATE SINK s1 FROM mv1 WITH (
    connector='jdbc',
    ...
);
```
