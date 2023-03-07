package com.risingwave.connector;

import static io.grpc.Status.*;

import com.risingwave.connector.api.sink.SinkFactory;

public class SinkUtils {
    public static SinkFactory getSinkFactory(String sinkType) {
        switch (sinkType) {
            case "print":
            case "connector-node-print":
                return new PrintSinkFactory();
            case "file":
                return new FileSinkFactory();
            case "jdbc":
                return new JDBCSinkFactory();
            case "iceberg":
                return new IcebergSinkFactory();
            case "deltalake":
                return new DeltaLakeSinkFactory();
            default:
                throw UNIMPLEMENTED
                        .withDescription("unknown sink type: " + sinkType)
                        .asRuntimeException();
        }
    }
}
