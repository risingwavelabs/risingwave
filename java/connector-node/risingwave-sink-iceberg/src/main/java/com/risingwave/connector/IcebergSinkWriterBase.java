package com.risingwave.connector;

import com.google.protobuf.ByteString;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.java.utils.ObjectSerde;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.Optional;

public abstract class IcebergSinkWriterBase implements SinkWriter {
    protected final TableSchema tableSchema;

    public IcebergSinkWriterBase(TableSchema tableSchema) {
        this.tableSchema = tableSchema;
    }

    @Override
    public void beginEpoch(long epoch) {}

    protected abstract IcebergMetadata collectSinkMetadata();

    @Override
    public Optional<ConnectorServiceProto.SinkMetadata> barrier(boolean isCheckpoint) {
        if (isCheckpoint) {
            IcebergMetadata metadata = collectSinkMetadata();
            return Optional.of(
                    ConnectorServiceProto.SinkMetadata.newBuilder()
                            .setSerialized(
                                    ConnectorServiceProto.SinkMetadata.SerializedMetadata
                                            .newBuilder()
                                            .setMetadata(
                                                    ByteString.copyFrom(
                                                            ObjectSerde.serializeObject(metadata)))
                                            .build())
                            .build());
        } else {
            return Optional.empty();
        }
    }
}
