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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.proto.Catalog.SinkType;
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
        ObjectMapper mapper = new ObjectMapper();
        IcebergSinkConfig config = mapper.convertValue(tableProperties, IcebergSinkConfig.class);
        String warehousePath = getWarehousePath(config);
        config.setWarehousePath(warehousePath);

        String scheme = parseWarehousePathScheme(warehousePath);
        TableIdentifier tableIdentifier =
                TableIdentifier.of(config.getDatabaseName(), config.getTableName());
        Configuration hadoopConf = createHadoopConf(scheme, config);
        SinkBase sink = null;

        try (HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath); ) {
            Table icebergTable = hadoopCatalog.loadTable(tableIdentifier);
            String sinkType = config.getSinkType();
            if (sinkType.equals("append-only")) {
                sink = new IcebergSink(tableSchema, hadoopCatalog, icebergTable, FILE_FORMAT);
            } else if (sinkType.equals("upsert")) {
                sink =
                        new UpsertIcebergSink(
                                tableSchema, hadoopCatalog,
                                icebergTable, FILE_FORMAT);
            }
        } catch (Exception e) {
            throw Status.FAILED_PRECONDITION
                    .withDescription(
                            String.format("failed to load iceberg table: %s", e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }

        if (sink == null) {
            throw UNIMPLEMENTED
                    .withDescription("unsupported mode: " + config.getSinkType())
                    .asRuntimeException();
        }
        return sink;
    }

    @Override
    public void validate(
            TableSchema tableSchema, Map<String, String> tableProperties, SinkType sinkType) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES, true);
        IcebergSinkConfig config = mapper.convertValue(tableProperties, IcebergSinkConfig.class);

        String warehousePath = getWarehousePath(config);
        String scheme = parseWarehousePathScheme(warehousePath);
        TableIdentifier tableIdentifier =
                TableIdentifier.of(config.getDatabaseName(), config.getTableName());
        Configuration hadoopConf = createHadoopConf(scheme, config);

        try (HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath); ) {

            Table icebergTable = hadoopCatalog.loadTable(tableIdentifier);

            // Check that all columns in tableSchema exist in the iceberg table.
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

            // Check that all required columns in the iceberg table exist in tableSchema.
            Set<String> columnNames = Set.of(tableSchema.getColumnNames());
            for (Types.NestedField column : icebergTable.schema().columns()) {
                if (column.isRequired() && !columnNames.contains(column.name())) {
                    throw Status.FAILED_PRECONDITION
                            .withDescription(
                                    String.format("missing a required field %s", column.name()))
                            .asRuntimeException();
                }
            }

        } catch (Exception e) {
            throw Status.INTERNAL
                    .withDescription(
                            String.format("failed to load iceberg table: %s", e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }

        if (!config.getSinkType().equals("append-only") && !config.getSinkType().equals("upsert")) {
            throw UNIMPLEMENTED
                    .withDescription("unsupported mode: " + config.getSinkType())
                    .asRuntimeException();
        }

        switch (sinkType) {
            case UPSERT:
                // For upsert iceberg sink, the user must specify its primary key explicitly.
                if (tableSchema.getPrimaryKeys().isEmpty()) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("please define primary key for upsert iceberg sink")
                            .asRuntimeException();
                }
                break;
            case APPEND_ONLY:
            case FORCE_APPEND_ONLY:
                break;
            default:
                throw Status.INTERNAL.asRuntimeException();
        }
    }

    private static String getWarehousePath(IcebergSinkConfig config) {
        String warehousePath = config.getWarehousePath();
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

    private Configuration createHadoopConf(String scheme, IcebergSinkConfig config) {
        switch (scheme) {
            case "file":
                return new Configuration();
            case "s3a":
                Configuration hadoopConf = new Configuration();
                hadoopConf.set(confIoImpl, s3FileIOImpl);
                hadoopConf.setBoolean(confPathStyleAccess, true);
                if (!config.hasS3Endpoint()) {
                    throw INVALID_ARGUMENT
                            .withDescription(
                                    String.format(
                                            "Should set `s3.endpoint` for warehouse with scheme %s",
                                            scheme))
                            .asRuntimeException();
                }
                hadoopConf.set(confEndpoint, config.getS3Endpoint());
                if (config.hasS3AccessKey()) {
                    hadoopConf.set(confKey, config.getS3AccessKey());
                }
                if (config.hasS3SecretKey()) {
                    hadoopConf.set(confSecret, config.getS3SecretKey());
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
