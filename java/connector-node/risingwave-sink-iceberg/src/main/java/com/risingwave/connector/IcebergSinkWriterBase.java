package com.risingwave.connector;

import com.google.protobuf.ByteString;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkCoordinator;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.java.utils.ObjectSerde;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.Collections;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopCatalog;

public abstract class IcebergSinkWriterBase implements SinkWriter {
    protected final TableSchema tableSchema;
    protected final SinkCoordinator coordinator;
    protected final HadoopCatalog hadoopCatalog;
    protected final Table icebergTable;
    protected final Schema rowSchema;
    protected final FileFormat fileFormat;
    private long epoch;

    public IcebergSinkWriterBase(
            TableSchema tableSchema,
            Table icebergTable,
            HadoopCatalog hadoopCatalog,
            Schema rowSchema,
            FileFormat fileFormat) {
        this.tableSchema = tableSchema;
        this.coordinator = new IcebergSinkCoordinator(icebergTable);
        this.hadoopCatalog = hadoopCatalog;
        this.rowSchema = rowSchema;
        this.icebergTable = icebergTable;
        this.fileFormat = fileFormat;
    }

    @Override
    public void beginEpoch(long epoch) {
        this.epoch = epoch;
    }

    protected abstract IcebergMetadata collectSinkMetadata();

    @Override
    public void barrier(boolean isCheckpoint) {
        if (isCheckpoint) {
            IcebergMetadata icebergMetadata = collectSinkMetadata();

            ConnectorServiceProto.SinkMetadata metadata =
                    ConnectorServiceProto.SinkMetadata.newBuilder()
                            .setSerialized(
                                    ConnectorServiceProto.SinkMetadata.SerializedMetadata
                                            .newBuilder()
                                            .setMetadata(
                                                    ByteString.copyFrom(
                                                            ObjectSerde.serializeObject(
                                                                    icebergMetadata)))
                                            .build())
                            .build();
            coordinator.commit(epoch, Collections.singletonList(metadata));
        }
    }

    public HadoopCatalog getHadoopCatalog() {
        return this.hadoopCatalog;
    }

    public Table getIcebergTable() {
        return this.icebergTable;
    }
}
