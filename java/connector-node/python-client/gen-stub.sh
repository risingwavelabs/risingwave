python3 -m grpc_tools.protoc -I../proto/src/main/proto   --python_out=. --grpc_python_out=. ../proto/src/main/proto/*.proto ../proto/src/main/proto/**/*.proto
