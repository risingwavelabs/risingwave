// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.connector;

import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.UNIMPLEMENTED;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import io.grpc.Status;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergSinkFactory implements SinkFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSinkFactory.class);

    public static final String SINK_MODE_PROP = "sink.mode";
    public static final String WAREHOUSE_PATH_PROP = "warehouse.path";
    public static final String DATABASE_NAME_PROP = "database.name";
    public static final String TABLE_NAME_PROP = "table.name";
    public static final String S3_ACCESS_KEY_PROP = "s3.access.key";
    public static final String S3_SECRET_KEY_PROP = "s3.secret.key";
    public static final String S3_ENDPOINT_PROP = "s3.endpoint";
    public static final FileFormat FILE_FORMAT = FileFormat.PARQUET;

    // hadoop catalog config
    private static final String confEndpoint = "fs.s3a.endpoint";
    private static final String confKey = "fs.s3a.access.key";
    private static final String confSecret = "fs.s3a.secret.key";
    private static final String confIoImpl = "io-impl";
    private static final String confPathStyleAccess = "fs.s3a.path.style.access";
    private static final String s3FileIOImpl = "org.apache.iceberg.aws.s3.S3FileIO";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        // TODO: Remove this call to `validate` after supporting sink validation in risingwave.
        validate(tableSchema, tableProperties);

        String mode = tableProperties.get(SINK_MODE_PROP);
        String warehousePath = getWarehousePath(tableProperties);
        String databaseName = tableProperties.get(DATABASE_NAME_PROP);
        String tableName = tableProperties.get(TABLE_NAME_PROP);

        String scheme = parseWarehousePathScheme(warehousePath);

        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        Configuration hadoopConf = createHadoopConf(scheme, tableProperties);
        HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath);
        Table icebergTable;
        try {
            icebergTable = hadoopCatalog.loadTable(tableIdentifier);
        } catch (Exception e) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(
                            String.format("failed to load iceberg table: %s", e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }

        if (mode.equals("append-only")) {
            return new IcebergSink(tableSchema, hadoopCatalog, icebergTable, FILE_FORMAT);
        } else if (mode.equals("upsert")) {
            return new UpsertIcebergSink(
                    tableSchema, hadoopCatalog,
                    icebergTable, FILE_FORMAT);
        }
        throw UNIMPLEMENTED.withDescription("unsupported mode: " + mode).asRuntimeException();
    }

    @Override
    public void validate(TableSchema tableSchema, Map<String, String> tableProperties) {
        if (!tableProperties.containsKey(SINK_MODE_PROP) // only append-only, upsert
                || !tableProperties.containsKey(WAREHOUSE_PATH_PROP)
                || !tableProperties.containsKey(DATABASE_NAME_PROP)
                || !tableProperties.containsKey(TABLE_NAME_PROP)) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "%s, %s, %s or %s is not specified",
                                    SINK_MODE_PROP,
                                    WAREHOUSE_PATH_PROP,
                                    DATABASE_NAME_PROP,
                                    TABLE_NAME_PROP))
                    .asRuntimeException();
        }

        String mode = tableProperties.get(SINK_MODE_PROP);
        String databaseName = tableProperties.get(DATABASE_NAME_PROP);
        String tableName = tableProperties.get(TABLE_NAME_PROP);
        String warehousePath = getWarehousePath(tableProperties);

        String schema = parseWarehousePathScheme(warehousePath);

        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        Configuration hadoopConf = createHadoopConf(schema, tableProperties);
        HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath);
        Table icebergTable;
        try {
            icebergTable = hadoopCatalog.loadTable(tableIdentifier);
        } catch (Exception e) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(
                            String.format("failed to load iceberg table: %s", e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }
        // check that all columns in tableSchema exist in the iceberg table
        for (String columnName : tableSchema.getColumnNames()) {
            if (icebergTable.schema().findField(columnName) == null) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format(
                                        "table schema does not match. Column %s not found in iceberg table",
                                        columnName))
                        .asRuntimeException();
            }
        }
        // check that all required columns in the iceberg table exist in tableSchema
        Set<String> columnNames = Set.of(tableSchema.getColumnNames());
        for (Types.NestedField column : icebergTable.schema().columns()) {
            if (column.isRequired() && !columnNames.contains(column.name())) {
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format("missing a required field %s", column.name()))
                        .asRuntimeException();
            }
        }

        if (!mode.equals("append-only") && !mode.equals("upsert")) {
            throw UNIMPLEMENTED.withDescription("unsupported mode: " + mode).asRuntimeException();
        }

        if (mode.equals("upsert")) {
            if (tableSchema.getPrimaryKeys().isEmpty()) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("no primary keys for upsert mode")
                        .asRuntimeException();
            }
        }
    }

    private static String getWarehousePath(Map<String, String> tableProperties) {
        String warehousePath = tableProperties.get(WAREHOUSE_PATH_PROP);
        // unify s3 and s3a
        if (warehousePath.startsWith("s3://")) {
            return warehousePath.replace("s3://", "s3a://");
        }
        return warehousePath;
    }

    private static String parseWarehousePathScheme(String warehousePath) {
        try {
            URI uri = new URI(warehousePath);
            String scheme = uri.getScheme();
            if (scheme == null) {
                throw INVALID_ARGUMENT
                        .withDescription("warehouse path should set scheme (e.g. s3a://)")
                        .asRuntimeException();
            }
            return scheme;
        } catch (URISyntaxException e) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            String.format("invalid warehouse path uri: %s", e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }
    }

    private Configuration createHadoopConf(String scheme, Map<String, String> tableProperties) {
        switch (scheme) {
            case "file":
                return new Configuration();
            case "s3a":
                Configuration hadoopConf = new Configuration();
                hadoopConf.set(confIoImpl, s3FileIOImpl);
                hadoopConf.setBoolean(confPathStyleAccess, true);
                if (!tableProperties.containsKey(S3_ENDPOINT_PROP)) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    String.format(
                                            "Should set %s for warehouse with scheme %s",
                                            S3_ENDPOINT_PROP, scheme))
                            .asRuntimeException();
                }
                hadoopConf.set(confEndpoint, tableProperties.get(S3_ENDPOINT_PROP));
                if (tableProperties.containsKey(S3_ACCESS_KEY_PROP)) {
                    hadoopConf.set(confKey, tableProperties.get(S3_ACCESS_KEY_PROP));
                }
                if (tableProperties.containsKey(S3_SECRET_KEY_PROP)) {
                    hadoopConf.set(confSecret, tableProperties.get(S3_SECRET_KEY_PROP));
                }
                return hadoopConf;
            default:
                throw UNIMPLEMENTED
                        .withDescription(
                                String.format("scheme %s not supported for warehouse path", scheme))
                        .asRuntimeException();
        }
    }
}
