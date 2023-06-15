RISINGWAVE_ROOT=$(git rev-parse --show-toplevel)
JAVA_ROOT=$RISINGWAVE_ROOT/java
CONNECTOR_ROOT=$JAVA_ROOT/connector-node
# Build shared library file
cd $RISINGWAVE_ROOT && cargo build -p risingwave_java_binding
# specify the Djava.library.path, please make sure the shared library you needed exist in /target/debug
cd ${CONNECTOR_ROOT}/assembly/target && java -classpath "./libs/*" -Djava.library.path=${RISINGWAVE_ROOT}/target/debug com.risingwave.connector.ConnectorService