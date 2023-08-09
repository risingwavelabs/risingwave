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

import com.google.protobuf.ByteString;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.SinkCoordinator;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.java.utils.ObjectSerde;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.Collections;
import java.util.Optional;
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
    public Optional<ConnectorServiceProto.SinkMetadata> barrier(boolean isCheckpoint) {
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
        return Optional.empty();
    }

    public HadoopCatalog getHadoopCatalog() {
        return this.hadoopCatalog;
    }

    public Table getIcebergTable() {
        return this.icebergTable;
    }
}
