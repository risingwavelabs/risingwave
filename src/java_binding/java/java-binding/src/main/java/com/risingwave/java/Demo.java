package com.risingwave.java;

import com.risingwave.java.binding.Iterator;
import com.risingwave.java.binding.KeyedRow;
import com.risingwave.java.binding.rpc.MetaClient;
import com.risingwave.proto.Catalog.Table;
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.JavaBinding.KeyRange;
import com.risingwave.proto.JavaBinding.KeyRange.Bound;
import com.risingwave.proto.JavaBinding.ReadPlan;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/** Hello world! */
public class Demo {
    public static void main(String[] args) {
        String objectStore = System.getenv("OBJECT_STORE");
        String dbName = System.getenv("DB_NAME");
        String tableName = System.getenv("TABLE_NAME");
        String metaAddr = System.getenv("META_ADDR");
        String dataDir = System.getenv("DATA_DIR");

        ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(2);

        KeyRange keyRange =
                KeyRange.newBuilder()
                        .setRightBound(Bound.UNBOUNDED)
                        .setLeftBound(Bound.UNBOUNDED)
                        .build();
        try (MetaClient metaClient = new MetaClient(metaAddr, scheduledThreadPool)) {
            ScheduledFuture<?> heartbeatFuture =
                    metaClient.startHeartbeatLoop(Duration.ofMillis(1000), Duration.ofSeconds(600));
            HummockVersion version = metaClient.pinVersion();
            Table tableCatalog = metaClient.getTable(dbName, tableName);
            ReadPlan readPlan =
                    ReadPlan.newBuilder()
                            .setDataDir(dataDir)
                            .setObjectStoreUrl(objectStore)
                            .setKeyRange(keyRange)
                            .setTableId(tableCatalog.getId())
                            .setEpoch(version.getMaxCommittedEpoch())
                            .setVersion(version)
                            .setTableCatalog(tableCatalog)
                            .build();

            try (Iterator iter = new Iterator(readPlan)) {
                while (true) {
                    try (KeyedRow row = iter.next()) {
                        if (row == null) {
                            break;
                        }
                        System.out.printf(
                                "key %s, id: %d, name: %s, is null: %s%n",
                                Arrays.toString(row.getKey()),
                                row.getLong(0),
                                row.getString(1),
                                row.isNull(2));
                    }
                }
            }

            heartbeatFuture.cancel(false);
        }

        scheduledThreadPool.shutdown();
    }
}
