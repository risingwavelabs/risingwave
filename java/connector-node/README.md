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
cd assembly/target && tar xvf risingwave-connector-1.0.0.tar.gz 
# launch connector node service
java -classpath "./libs/*" com.risingwave.connector.ConnectorService
```

Sometimes, you need to specify the shared library path. For example, when program want to run class related to java-binding, they need to call shared library file. If not, it will throw exception when running. You need:

```
RISINGWAVE_ROOT=$(git rev-parse --show-toplevel)
CONNECTOR_ROOT=$JAVA_ROOT/connector-node
cd ${CONNECTOR_ROOT}/assembly/target && java -classpath "./libs/*" com.risingwave.connector.ConnectorService
```

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
