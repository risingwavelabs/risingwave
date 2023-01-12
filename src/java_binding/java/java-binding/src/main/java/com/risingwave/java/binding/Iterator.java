package com.risingwave.java.binding;

import com.risingwave.java.binding.rpc.MetaClient;
import com.risingwave.proto.Catalog.Table;
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.JavaBinding.ReadPlan;
import java.time.Duration;

public class Iterator implements AutoCloseable {
    final long pointer;
    final MetaClient metaClient;
    final Table catalog;
    final long versionId;
    boolean isClosed;

    public Iterator(MetaClient metaClient, String stateStore, String dbName, String tableName) {
        metaClient.startHeartbeatLoop(Duration.ofMillis(1000), Duration.ofSeconds(600));
        HummockVersion version = metaClient.pinVersion();
        Table tableCatalog = metaClient.getTable(dbName, tableName);
        ReadPlan readPlan =
                ReadPlan.newBuilder()
                        .setVersion(version)
                        .setTableId(tableCatalog.getId())
                        .setTableCatalog(tableCatalog)
                        .build();

        this.metaClient = metaClient;
        this.pointer = Binding.iteratorNew(readPlan.toByteArray(), stateStore);
        this.catalog = tableCatalog;
        this.versionId = version.getId();
        this.isClosed = false;
    }

    public Record next() {
        long pointer = Binding.iteratorNext(this.pointer);
        if (pointer == 0) {
            return null;
        }
        return new Record(pointer);
    }

    @Override
    public void close() {
        // TODO: unpin version
        if (!isClosed) {
            isClosed = true;
            Binding.iteratorClose(pointer);
        }
    }
}
