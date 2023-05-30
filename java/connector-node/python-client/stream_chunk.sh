#!/usr/bin/env bash
RISINGWAVE_ROOT=$(git rev-parse --show-toplevel)
JAVA_ROOT=$RISINGWAVE_ROOT/java
CONNECTOR_ROOT=$JAVA_ROOT/connector-node
PYTHON_DIRECTORY=${RISINGWAVE_ROOT}/java/connector-node/python-client
DATA_FILE=${PYTHON_DIRECTORY}/data/stream_chunk_data

# # Launch the service

# # compile conenctor-node amd java-binding
# cd $JAVA_ROOT && mvn clean package -DskipTests=true

# # unpack the tar file, the file name might vary depending on the version
# cd ${CONNECTOR_ROOT}/assembly/target && tar xvf risingwave-connector-1.0.0.tar.gz 

# # compile java-binding *.so file
# cd $RISINGWAVE_ROOT && cargo build -p risingwave_java_binding

# # launch connector node service, make sure to set Djava.library.path, and the java-binding share library file should in this path
# cd ${CONNECTOR_ROOT}/assembly/target && java -classpath "./libs/*" -Djava.library.path=${RISINGWAVE_ROOT}/target/debug com.risingwave.connector.ConnectorService

# Generate data
cd ${RISINGWAVE_ROOT} && cargo run --bin data-chunk-payload-generator unit-test > ${DATA_FILE}

# Run unit test
cd $PYTHON_DIRECTORY
python integration_tests.py --stream_chunk_sink --input_binary_file ./data/stream_chunk_data

# Remove data
rm $DATA_FILE