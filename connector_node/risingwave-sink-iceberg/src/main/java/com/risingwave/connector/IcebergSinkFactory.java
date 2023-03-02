package com.risingwave.connector;

import static io.grpc.Status.INVALID_ARGUMENT;
import static io.grpc.Status.UNIMPLEMENTED;

import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkBase;
import com.risingwave.connector.api.sink.SinkFactory;
import com.risingwave.connector.utils.MinioUrlParser;
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

    public static final String SINK_MODE_PROP = "sink.mode";
    public static final String LOCATION_TYPE_PROP = "location.type";
    public static final String WAREHOUSE_PATH_PROP = "warehouse.path";
    public static final String DATABASE_NAME_PROP = "database.name";
    public static final String TABLE_NAME_PROP = "table.name";
    public static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
    private static final String confEndpoint = "fs.s3a.endpoint";
    private static final String confKey = "fs.s3a.access.key";
    private static final String confSecret = "fs.s3a.secret.key";
    private static final String confIoImpl = "io-impl";
    private static final String confPathStyleAccess = "fs.s3a.path.style.access";
    private static final String s3FileIOImpl = "org.apache.iceberg.aws.s3.S3FileIO";

    @Override
    public SinkBase create(TableSchema tableSchema, Map<String, String> tableProperties) {
        if (!tableProperties.containsKey(SINK_MODE_PROP) // only append-only, upsert
                || !tableProperties.containsKey(LOCATION_TYPE_PROP) // only local, s3, minio
                || !tableProperties.containsKey(WAREHOUSE_PATH_PROP)
                || !tableProperties.containsKey(DATABASE_NAME_PROP)
                || !tableProperties.containsKey(TABLE_NAME_PROP)) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            String.format(
                                    "%s, %s, %s, %s or %s is not specified",
                                    SINK_MODE_PROP,
                                    LOCATION_TYPE_PROP,
                                    WAREHOUSE_PATH_PROP,
                                    DATABASE_NAME_PROP,
                                    TABLE_NAME_PROP))
                    .asRuntimeException();
        }

        String mode = tableProperties.get(SINK_MODE_PROP);
        String location = tableProperties.get(LOCATION_TYPE_PROP);
        String warehousePath = tableProperties.get(WAREHOUSE_PATH_PROP);
        String databaseName = tableProperties.get(DATABASE_NAME_PROP);
        String tableName = tableProperties.get(TABLE_NAME_PROP);

        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        HadoopCatalog hadoopCatalog = createHadoopCatalog(location, warehousePath);
        Table icebergTable;
        try {
            icebergTable = hadoopCatalog.loadTable(tableIdentifier);
        } catch (Exception e) {
            LOG.error("load table error: {}", e);
            throw Status.FAILED_PRECONDITION
                    .withDescription("failed to load iceberg table")
                    .withCause(e)
                    .asRuntimeException();
        }
        // check that all columns in tableSchema exist in the iceberg table
        for (String columnName : tableSchema.getColumnNames()) {
            if (icebergTable.schema().findField(columnName) == null) {
                LOG.error("column not found: {}", columnName);
                throw Status.FAILED_PRECONDITION
                        .withDescription("table schema does not match")
                        .asRuntimeException();
            }
        }
        // check that all required columns in the iceberg table exist in tableSchema
        Set<String> columnNames = Set.of(tableSchema.getColumnNames());
        for (Types.NestedField column : icebergTable.schema().columns()) {
            if (column.isRequired() && !columnNames.contains(column.name())) {
                LOG.error("required column not found: {}", column.name());
                throw Status.FAILED_PRECONDITION
                        .withDescription(
                                String.format("missing a required field %s", column.name()))
                        .asRuntimeException();
            }
        }

        if (mode.equals("append-only")) {
            return new IcebergSink(tableSchema, hadoopCatalog, icebergTable, FILE_FORMAT);
        }

        if (mode.equals("upsert")) {
            if (tableSchema.getPrimaryKeys().isEmpty()) {
                throw Status.FAILED_PRECONDITION
                        .withDescription("no primary keys for upsert mode")
                        .asRuntimeException();
            }
            return new UpsertIcebergSink(tableSchema, hadoopCatalog, icebergTable, FILE_FORMAT);
        }

        throw UNIMPLEMENTED.withDescription("unsupported mode: " + mode).asRuntimeException();
    }

    private HadoopCatalog createHadoopCatalog(String location, String warehousePath) {
        Configuration hadoopConf = new Configuration();
        switch (location) {
            case "local":
                return new HadoopCatalog(hadoopConf, warehousePath);
            case "s3":
                hadoopConf.set(confIoImpl, s3FileIOImpl);
                String s3aPath = "s3a:" + warehousePath.substring(warehousePath.indexOf('/'));
                return new HadoopCatalog(hadoopConf, s3aPath);
            case "minio":
                hadoopConf.set(confIoImpl, s3FileIOImpl);
                MinioUrlParser minioUrlParser = new MinioUrlParser(warehousePath);
                hadoopConf.set(confEndpoint, minioUrlParser.getEndpoint());
                hadoopConf.set(confKey, minioUrlParser.getKey());
                hadoopConf.set(confSecret, minioUrlParser.getSecret());
                hadoopConf.setBoolean(confPathStyleAccess, true);
                return new HadoopCatalog(hadoopConf, "s3a://" + minioUrlParser.getBucket());
            default:
                throw UNIMPLEMENTED
                        .withDescription("unsupported iceberg sink type: " + location)
                        .asRuntimeException();
        }
    }
}
