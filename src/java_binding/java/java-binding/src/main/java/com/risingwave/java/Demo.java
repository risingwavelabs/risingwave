package com.risingwave.java;

import com.risingwave.java.binding.Iterator;
import com.risingwave.java.binding.Record;
import com.risingwave.java.binding.rpc.MetaClient;
import java.util.Arrays;
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
            while (true) {
                try (Record record = iter.next()) {
                    if (record == null) {
                        break;
                    }
                    System.out.printf(
                            "key %s, id: %d, name: %s, is null: %s%n",
                            Arrays.toString(record.getKey()),
                            record.getLong(0),
                            record.getString(1),
                            record.isNull(2));
                }
            }
        }

        scheduledThreadPool.shutdown();
    }
}
