package com.risingwave.connector.api;

import com.risingwave.proto.Data;
import io.grpc.Status;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

public class FlinkDynamicUtil {
    public static DataType getCorrespondingFlinkType(Data.DataType dataType) {
        switch (dataType.getTypeName()) {
            case INT16:
                return DataTypes.SMALLINT();
            case INT32:
                return DataTypes.INT();
            case INT64:
                return DataTypes.BIGINT();
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case VARCHAR:
                return DataTypes.STRING();
            case DECIMAL:
                // Because there is no 'scale' and 'precision' in 'DECIMAL' within RW,
                // this is just a simulation here, which may be incompatible with some interfaces.
                return DataTypes.DECIMAL(9, 1);
            case TIMESTAMP:
                // Like DECIMAL
                return DataTypes.TIMESTAMP();
            case TIMESTAMPTZ:
                // Like DECIMAL
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
            case DATE:
                return DataTypes.DATE();
            case TIME:
                return DataTypes.TIME();
            case BYTEA:
                return DataTypes.BYTES();
            case LIST:
            case STRUCT:
            case INTERVAL:
                throw Status.UNIMPLEMENTED
                        .withDescription(String.format("not support %s now", dataType))
                        .asRuntimeException();
            default:
                throw Status.INVALID_ARGUMENT
                        .withDescription("unspecified type" + dataType)
                        .asRuntimeException();
        }
    }
}
