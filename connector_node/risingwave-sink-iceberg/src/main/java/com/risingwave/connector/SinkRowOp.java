package com.risingwave.connector;

import com.risingwave.connector.api.sink.SinkRow;
import io.grpc.Status;

public class SinkRowOp {
    private final SinkRow delete;
    private final SinkRow insert;

    public static SinkRowOp insertOp(SinkRow row) {
        if (row == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("row op must not be null to initialize insertOp")
                    .asRuntimeException();
        }
        return new SinkRowOp(null, row);
    }

    public static SinkRowOp deleteOp(SinkRow row) {
        if (row == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("row op must not be null to initialize deleteOp")
                    .asRuntimeException();
        }
        return new SinkRowOp(row, null);
    }

    public static SinkRowOp updateOp(SinkRow delete, SinkRow insert) {
        if (delete == null || insert == null) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("row ops must not be null initialize updateOp")
                    .asRuntimeException();
        }
        return new SinkRowOp(delete, insert);
    }

    private SinkRowOp(SinkRow delete, SinkRow insert) {
        this.delete = delete;
        this.insert = insert;
    }

    public boolean isDelete() {
        return insert == null && delete != null;
    }

    public SinkRow getDelete() {
        return delete;
    }

    public SinkRow getInsert() {
        return insert;
    }
}
