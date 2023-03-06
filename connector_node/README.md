# RisingWave Connector Node

The RisingWave Connector Node is a connector service that bundles customizable external sinks, allowing you to easily connect to various data sources and sinks. It acts as a bridge between RisingWave and the external systems, enabling you to stream data bidirectionally between them.

## Up and running

To build the Connector Node, you will need to have Maven and Python 3 installed on your system. On Ubuntu, you can install these dependencies using the package manager:

```
sudo apt-get update
sudo apt-get install maven python3
```
To build the Connector Node, run the following command from the project's root directory:

```
mvn clean package
```

This will create a `.tar.gz` file with the Connector Node and all its dependencies in the `assembly/target` directory. To run the Connector Node, execute the following command:

```
# unpack the tar file, the file name might vary depending on the version
cd assembly/target && tar xvf risingwave-connector-1.0.0.tar.gz 
# launch connector node service
java -classpath "./libs/*" com.risingwave.connector.ConnectorService
```

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
