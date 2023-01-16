package com.risingwave.java;

import com.risingwave.java.binding.Iterator;
import com.risingwave.java.binding.KeyedRow;
import com.risingwave.java.binding.rpc.MetaClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/** Hello world! */
public class Demo {
    public static void main(String[] args) {
        String stateStore = System.getenv("STATE_STORE");
        String dbName = System.getenv("DB_NAME");
        String tableName = System.getenv("TABLE_NAME");
        String metaAddr = System.getenv("META_ADDR");

        ScheduledThreadPoolExecutor scheduledThreadPool = new ScheduledThreadPoolExecutor(2);

        try (MetaClient metaClient = new MetaClient(metaAddr, scheduledThreadPool);
                Iterator iter = new Iterator(metaClient, stateStore, dbName, tableName)) {
            RunnableScheduledFuture<?> heartbeatFuture =
                    (RunnableScheduledFuture<?>)
                            metaClient.startHeartbeatLoop(
                                    Duration.ofMillis(1000), Duration.ofSeconds(600));

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

            scheduledThreadPool.remove(heartbeatFuture);
        }

        scheduledThreadPool.shutdown();
    }
}
