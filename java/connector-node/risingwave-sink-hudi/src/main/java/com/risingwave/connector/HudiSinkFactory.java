/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.connector;

import static io.grpc.Status.*;
import static org.apache.avro.Schema.Type.UNION;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.common.S3Utils;
import com.risingwave.java.utils.UrlParser;
import com.risingwave.proto.Catalog;
import com.risingwave.proto.Data;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

public class HudiSinkFactory implements SinkFactory {

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        ObjectMapper mapper = new ObjectMapper();
        HudiSinkConfig config = mapper.convertValue(tableProperties, HudiSinkConfig.class);

        return new HudiSink(
                config.getBasePath(), config.getTableName(), getHadoopConf(config), tableSchema);
    }

    @Override
    public void validate(
            TableSchema tableSchema,
            Map<String, String> tableProperties,
            Catalog.SinkType sinkType) {

        ObjectMapper mapper = new ObjectMapper();
        HudiSinkConfig config = mapper.convertValue(tableProperties, HudiSinkConfig.class);
        Configuration hadoopConf = getHadoopConf(config);

        HoodieTableMetaClient client = loadTableMetaClient(config.getBasePath(), hadoopConf);
        try {
            if (!FSUtils.isTableExists(config.getBasePath(), client.getRawFs())) {
                throw INVALID_ARGUMENT
                        .withDescription(
                                String.format(
                                        "No hudi table exists at base path %s",
                                        config.getBasePath()))
                        .asRuntimeException();
            }
        } catch (IOException e) {
            throw INVALID_ARGUMENT
                    .withCause(e)
                    .withDescription(
                            String.format("failed to check whether table exists not not: %s", e))
                    .asRuntimeException();
        }

        if (!client.getTableConfig().getTableType().equals(HoodieTableType.MERGE_ON_READ)) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "Table at location %s is not a MERGE_ON_READ hudi table",
                                    config.getBasePath()))
                    .asRuntimeException();
        }

        if (!client.getTableConfig().getRecordKeyFields().isPresent()) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "Table at location %s does not have record key configured",
                                    config.getBasePath()))
                    .asRuntimeException();
        }

        // Check record key and RisingWave primary key
        String[] recordKeyFields = client.getTableConfig().getRecordKeyFields().get();
        if (recordKeyFields.length != tableSchema.getPrimaryKeys().size()) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            "Length of record key schema does not match the length of primary key")
                    .asRuntimeException();
        }
        for (int i = 0; i < recordKeyFields.length; i++) {
            String hudiKeyName = recordKeyFields[i];
            String risingWaveKeyName = tableSchema.getPrimaryKeys().get(i);
            if (!hudiKeyName.equals(risingWaveKeyName)) {
                throw INVALID_ARGUMENT
                        .withDescription(
                                String.format(
                                        "Hudi primary key name %s with at position %s not match with RisingWave key name %s",
                                        hudiKeyName, i, risingWaveKeyName))
                        .asRuntimeException();
            }
        }

        if (!client.getTableConfig().getTableCreateSchema().isPresent()) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "Table at location %s does not have table create schema",
                                    config.getBasePath()))
                    .asRuntimeException();
        }

        // Check whether the type match the schema
        Schema schema = getTableSchema(client);
        if (schema.getFields().size() != tableSchema.getNumColumns()) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            "Length of hudi schema does not match the length of RisingWave schema")
                    .asRuntimeException();
        }

        for (int i = 0; i < schema.getFields().size(); i++) {
            Schema.Field field = schema.getFields().get(i);
            String risingWaveFieldName = tableSchema.getColumnNames()[i];
            if (!field.name().equals(risingWaveFieldName)) {
                throw INVALID_ARGUMENT
                        .withDescription(
                                String.format(
                                        "Hudi field name %s with at position %s not match with RisingWave field name %s",
                                        field.name(), i, risingWaveFieldName))
                        .asRuntimeException();
            }

            Schema hudiType = getActualSchemaFromUnion(field.schema());
            Data.DataType.TypeName risingWaveType = tableSchema.getColumnType(risingWaveFieldName);
            if (!equalType(hudiType, risingWaveType)) {
                throw INVALID_ARGUMENT
                        .withDescription(
                                String.format(
                                        "Hudi field type %s with at position %s not match with RisingWave field type %s",
                                        hudiType, i, risingWaveType))
                        .asRuntimeException();
            }
        }
    }

    static boolean equalType(Schema hudiFieldType, Data.DataType.TypeName risingWaveType) {
        switch (risingWaveType) {
            case INT16:
            case INT32:
                return hudiFieldType.getType().equals(Schema.Type.INT);
            case INT64:
                return hudiFieldType.getType().equals(Schema.Type.LONG);
            case VARCHAR:
                return hudiFieldType.getType().equals(Schema.Type.STRING);
            case FLOAT:
                return hudiFieldType.getType().equals(Schema.Type.FLOAT);
            case DOUBLE:
                return hudiFieldType.getType().equals(Schema.Type.DOUBLE);
            case BOOLEAN:
                return hudiFieldType.getType().equals(Schema.Type.BOOLEAN);
            case DECIMAL:
                return hudiFieldType.getLogicalType() instanceof LogicalTypes.Decimal;
            case TIMESTAMP:
            case TIMESTAMPTZ:
                return hudiFieldType.getLogicalType().equals(LogicalTypes.timestampMicros())
                        || hudiFieldType.getLogicalType().equals(LogicalTypes.timestampMillis());
            default:
                throw INVALID_ARGUMENT
                        .withDescription(
                                String.format(
                                        "unsupported data type for hudi sink: %s", risingWaveType))
                        .asRuntimeException();
        }
    }

    static HoodieTableMetaClient loadTableMetaClient(String basePath, Configuration hadoopConf) {
        return HoodieTableMetaClient.builder().setBasePath(basePath).setConf(hadoopConf).build();
    }

    static Schema getTableSchema(HoodieTableMetaClient client) {
        Option<Schema> schemaOpt = client.getTableConfig().getTableCreateSchema();
        if (!schemaOpt.isPresent()) {
            throw INTERNAL.withDescription("get table schema not set").asRuntimeException();
        }
        Schema schema = schemaOpt.get();
        return HoodieAvroUtils.removeMetadataFields(schema);
    }

    // Copied from org.apache.hudi.avro.HoodieAvroUtils.getActualSchemaFromUnion
    private static Schema getActualSchemaFromUnion(Schema schema) {
        if (!schema.getType().equals(UNION)) {
            return schema;
        }
        if (schema.getTypes().size() == 2
                && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
            return schema.getTypes().get(1);
        } else if (schema.getTypes().size() == 2
                && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
            return schema.getTypes().get(0);
        } else if (schema.getTypes().size() == 1) {
            return schema.getTypes().get(0);
        } else {
            throw INVALID_ARGUMENT
                    .withDescription(String.format("Unsupported field type: %s", schema))
                    .asRuntimeException();
        }
    }

    static Configuration getHadoopConf(HudiSinkConfig config) {
        String scheme = UrlParser.parseLocationScheme(config.getBasePath());
        switch (scheme) {
            case "file":
                return new Configuration();
            case "s3":
            case "s3a":
                return S3Utils.getHadoopConf(config);
            default:
                throw UNIMPLEMENTED
                        .withDescription(
                                String.format("scheme %s not supported for base path", scheme))
                        .asRuntimeException();
        }
    }
}
