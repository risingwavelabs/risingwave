package com.risingwave.java.binding;

import com.risingwave.java.utils.MetaClient;
import com.risingwave.proto.Catalog.Table;
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.JavaBinding.KeyRange;
import com.risingwave.proto.JavaBinding.KeyRange.Bound;
import com.risingwave.proto.JavaBinding.ReadPlan;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
                    metaClient.startHeartbeatLoop(Duration.ofMillis(1000));
            HummockVersion version = metaClient.pinVersion();
            Table tableCatalog = metaClient.getTable(dbName, tableName);

            int vnodeCount = Binding.vnodeCount();

            List<Integer> vnodeList = new ArrayList<>();
            for (int i = 0; i < vnodeCount; i++) {
                vnodeList.add(i);
            }

            ReadPlan readPlan =
                    ReadPlan.newBuilder()
                            .setDataDir(dataDir)
                            .setObjectStoreUrl(objectStore)
                            .setKeyRange(keyRange)
                            .setTableId(tableCatalog.getId())
                            .setEpoch(version.getMaxCommittedEpoch())
                            .setVersion(version)
                            .setTableCatalog(tableCatalog)
                            .addAllVnodeIds(vnodeList)
                            .build();

            try (Iterator iter = new Iterator(readPlan)) {
                while (true) {
                    try (KeyedRow row = iter.next()) {
                        if (row == null) {
                            break;
                        }
                        System.out.printf(
                                "key %s, smallint: %s, int: %s, bigint: %s, float: %s, double: %s, bool: %s, varchar: %s, is null: %s%n",
                                Arrays.toString(row.getKey()),
                                row.getShort(0),
                                row.getInt(1),
                                row.getLong(2),
                                row.getFloat(3),
                                row.getDouble(4),
                                row.getBoolean(5),
                                row.getString(6),
                                row.isNull(7));
                    }
                }
            }

            heartbeatFuture.cancel(false);
        }

        scheduledThreadPool.shutdown();
    }
}
