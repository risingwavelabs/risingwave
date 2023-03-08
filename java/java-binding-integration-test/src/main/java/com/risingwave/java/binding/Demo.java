package com.risingwave.java.binding;

import com.risingwave.java.utils.MetaClient;
import com.risingwave.proto.Catalog.Table;
import com.risingwave.proto.Hummock.HummockVersion;
import com.risingwave.proto.JavaBinding.KeyRange;
import com.risingwave.proto.JavaBinding.KeyRange.Bound;
import com.risingwave.proto.JavaBinding.ReadPlan;
import java.time.Duration;
import java.util.ArrayList;
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
                int count = 0;
                while (true) {
                    try (KeyedRow row = iter.next()) {
                        if (row == null) {
                            break;
                        }
                        count += 1;
                        validateRow(row);
                    }
                }
                int expectedCount = 30000;
                if (count != expectedCount) {
                    throw new RuntimeException(
                            String.format("row count is %s, should be %s", count, expectedCount));
                }
            }

            metaClient.unpinVersion(version);

            heartbeatFuture.cancel(false);
        }

        scheduledThreadPool.shutdown();
    }

    static void validateRow(KeyedRow row) {
        // The validation of row data are according to the data generation rule
        // defined in ${REPO_ROOT}/src/java_binding/gen-demo-insert-data.py
        short rowIndex = row.getShort(0);
        if (row.getInt(1) != rowIndex) {
            throw new RuntimeException(
                    String.format("invalid int value: %s %s", row.getInt(1), rowIndex));
        }
        if (row.getLong(2) != rowIndex) {
            throw new RuntimeException(
                    String.format("invalid long value: %s %s", row.getLong(2), rowIndex));
        }
        if (row.getFloat(3) != (float) rowIndex) {
            throw new RuntimeException(
                    String.format("invalid float value: %s %s", row.getFloat(3), rowIndex));
        }
        if (row.getDouble(4) != (double) rowIndex) {
            throw new RuntimeException(
                    String.format("invalid double value: %s %s", row.getDouble(4), rowIndex));
        }
        if (row.getBoolean(5) != (rowIndex % 3 == 0)) {
            throw new RuntimeException(
                    String.format(
                            "invalid bool value: %s %s", row.getBoolean(5), (rowIndex % 3 == 0)));
        }
        if (!row.getString(6).equals(((Short) rowIndex).toString().repeat((rowIndex % 10) + 1))) {
            throw new RuntimeException(
                    String.format(
                            "invalid string value: %s %s",
                            row.getString(6),
                            ((Short) rowIndex).toString().repeat((rowIndex % 10) + 1)));
        }
        if (row.isNull(7) != (rowIndex % 5 == 0)) {
            throw new RuntimeException(
                    String.format(
                            "invalid isNull value: %s %s", row.isNull(7), (rowIndex % 5 == 0)));
        }
    }
}
