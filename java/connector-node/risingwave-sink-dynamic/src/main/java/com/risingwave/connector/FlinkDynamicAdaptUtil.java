package com.risingwave.connector;

import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.proto.Data;
import io.grpc.Status;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.types.DataType;

public class FlinkDynamicAdaptUtil {
    public static void checkSchema(
            List<ColumnDesc> rwColumnDescs, List<Schema.UnresolvedColumn> flinkColumns) {
        if (rwColumnDescs.size() != flinkColumns.size()) {
            throw Status.FAILED_PRECONDITION
                    .withDescription("Don't match in the number of columns in the table")
                    .asRuntimeException();
        }

        Map<String, DataType> flinkColumnMap =
                flinkColumns.stream()
                        .map(
                                c -> {
                                    if (c instanceof Schema.UnresolvedPhysicalColumn) {
                                        Schema.UnresolvedPhysicalColumn c1 =
                                                (Schema.UnresolvedPhysicalColumn) c;
                                        return Map.entry(c1.getName(), c1.getDataType());
                                    } else {
                                        throw new RuntimeException("Only support physical column");
                                    }
                                })
                        .collect(Collectors.toMap(e -> e.getKey(), e -> (DataType) e.getValue()));

        for (ColumnDesc columnDesc : rwColumnDescs) {
            if (!flinkColumnMap.containsKey(columnDesc.getName())) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the name, rw is %s", columnDesc.getName()))
                        .asRuntimeException();
            }
            if (!getCorrespondingFlinkType(
                    columnDesc.getDataType(), flinkColumnMap.get(columnDesc.getName()))) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "Don't match in the type, name is %s, Sink is %s, rw is %s",
                                        columnDesc.getName(),
                                        flinkColumnMap.get(columnDesc.getName()),
                                        columnDesc.getDataType().getTypeName()))
                        .asRuntimeException();
            }
        }
    }

    private static boolean getCorrespondingFlinkType(Data.DataType dataType, DataType flinkType) {
        switch (dataType.getTypeName()) {
            case INT16:
                return DataTypes.SMALLINT().equals(flinkType);
            case INT32:
                return DataTypes.INT().equals(flinkType);
            case INT64:
                return DataTypes.BIGINT().equals(flinkType);
            case FLOAT:
                return DataTypes.FLOAT().equals(flinkType);
            case DOUBLE:
                return DataTypes.DOUBLE().equals(flinkType);
            case BOOLEAN:
                return DataTypes.BOOLEAN().equals(flinkType);
            case VARCHAR:
                System.out.println(flinkType.toString() + DataTypes.STRING().toString());
                return DataTypes.STRING().equals(flinkType);
            case DECIMAL:
                return flinkType.toString().contains("DECIMAL");
            case TIMESTAMP:
                return flinkType.toString().contains("TIMESTAMP");
            case TIMESTAMPTZ:
                String pattern = "TIMESTAMP\\(\\d+\\) WITH TIME ZONE";
                Pattern regex = Pattern.compile(pattern);
                Matcher matcher = regex.matcher(flinkType.toString());
                return matcher.matches();
            case DATE:
                return DataTypes.DATE().equals(flinkType);
            case TIME:
                return DataTypes.TIME().equals(flinkType);
            case BYTEA:
                return DataTypes.BYTES().equals(flinkType);
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
