package com.risingwave.connector;

import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.util.*;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.types.DataType;

public class FlinkDynamicAdaptUtil {

    public static FlinkSinkTableSchemaFinder discoverSchemaFinder(
            TableSchema tableSchema, FlinkDynamicAdaptConfig config) {
        if (config.getConnector().equals("http-sink")) {
            return new HttpTableSchemaFinder(tableSchema, config);
        }
        throw new RuntimeException("Cannot support connector" + config.getConnector());
    }

    public static List<Column> getFlinkColumnsFromSchema(TableSchema tableSchema) {
        List<Column> columns = new ArrayList<>();
        for (ColumnDesc columnDesc : tableSchema.getColumnDescs()) {
            columns.add(
                    Column.physical(
                            columnDesc.getName(),
                            getCorrespondingFlinkType(columnDesc.getDataType())));
        }
        return columns;
    }

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
                return DataTypes.DECIMAL(9, 1);
            case TIMESTAMP:
                return DataTypes.TIMESTAMP();
            case TIMESTAMPTZ:
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
