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

import static io.grpc.Status.UNIMPLEMENTED;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.common.S3Utils;
import com.risingwave.java.utils.UrlParser;
import com.risingwave.proto.Catalog.SinkType;
import com.risingwave.proto.Data.DataType.TypeName;
import io.grpc.Status;

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

    private static final String confIoImpl = "io-impl";
    private static final String s3FileIOImpl = "org.apache.iceberg.aws.s3.S3FileIO";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        IcebergSinkConfig config = mapper.convertValue(tableProperties, IcebergSinkConfig.class);
        String warehousePath = getWarehousePath(config);
        config.setWarehousePath(warehousePath);

        String scheme = UrlParser.parseLocationScheme(warehousePath);
        TableIdentifier tableIdentifier =
                TableIdentifier.of(config.getDatabaseName(), config.getTableName());
        Configuration hadoopConf = createHadoopConf(scheme, config);
        SinkBase sink = null;

        try (HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath);) {
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
        String scheme = UrlParser.parseLocationScheme(warehousePath);
        TableIdentifier tableIdentifier =
                TableIdentifier.of(config.getDatabaseName(), config.getTableName());
        Configuration hadoopConf = createHadoopConf(scheme, config);

        try (HadoopCatalog hadoopCatalog = new HadoopCatalog(hadoopConf, warehousePath);) {
            Table icebergTable = hadoopCatalog.loadTable(tableIdentifier);
            IcebergSinkUtil.checkSchema(tableSchema, icebergTable.schema());
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

    private Configuration createHadoopConf(String scheme, IcebergSinkConfig config) {
        switch (scheme) {
            case "file":
                return new Configuration();
            case "s3a":
                Configuration hadoopConf = S3Utils.getHadoopConf(config);
                hadoopConf.set(confIoImpl, s3FileIOImpl);
                return hadoopConf;
            default:
                throw UNIMPLEMENTED
                        .withDescription(
                                String.format("scheme %s not supported for warehouse path", scheme))
                        .asRuntimeException();
        }
    }
}
