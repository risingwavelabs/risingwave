export RISINGWAVE_PROTO_DIR=proto
export CONNECTOR_PROTO_DIR=connector_node/proto/src/main/proto/risingwave

apt install clang-format -y -qq
clang-format -i -Werror --style="{BasedOnStyle: Google, ReflowComments: false}" $RISINGWAVE_PROTO_DIR/*.proto


export HEADER_JAVA="option java_package = \"com.risingwave.proto\";"
export EXCL_PATTERN="($HEADER_JAVA|import|//)"

for file in $(find $CONNECTOR_PROTO_DIR -type f -name '*.proto'); do
  echo "checking " $file

  if ! diff <(egrep -v "$EXCL_PATTERN" $file) <(egrep -v "$EXCL_PATTERN" "$RISINGWAVE_PROTO_DIR/$(basename $file)") > /dev/null; then
    echo "proto file $file not consistent with original"
    diff <(egrep -v "$EXCL_PATTERN" $file) <(egrep -v "$EXCL_PATTERN" "$RISINGWAVE_PROTO_DIR/$(basename $file)")
    exit 1
  fi
done
echo "check OK: proto files are consistent with original"
