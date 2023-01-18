package com.risingwave.java.binding;

import com.risingwave.java.binding.rpc.MetaClient;
import com.risingwave.proto.Catalog.Table;
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.JavaBinding.KeyRange;
import com.risingwave.proto.JavaBinding.ReadPlan;

public class Iterator implements AutoCloseable {
    private final long pointer;
    private boolean isClosed;

    public Iterator(
            MetaClient metaClient,
            String stateStore,
            String dbName,
            String tableName,
            long epoch,
            KeyRange keyRange) {
        // Reply on context invalidation to unpin the version.
        HummockVersion version = metaClient.pinVersion();
        Table tableCatalog = metaClient.getTable(dbName, tableName);
        ReadPlan readPlan =
                ReadPlan.newBuilder()
                        .setTableId(tableCatalog.getId())
                        .setEpoch(epoch)
                        .setKeyRange(keyRange)
                        .setVersion(version)
                        .setTableCatalog(tableCatalog)
                        .build();

        this.pointer = Binding.iteratorNew(readPlan.toByteArray(), stateStore);
        this.isClosed = false;
    }

    public KeyedRow next() {
        long pointer = Binding.iteratorNext(this.pointer);
        if (pointer == 0) {
            return null;
        }
        return new KeyedRow(pointer);
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            Binding.iteratorClose(pointer);
        }
    }
}
