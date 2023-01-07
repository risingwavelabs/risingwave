package com.risingwave.java;

import com.risingwave.java.binding.Iterator;
import com.risingwave.java.binding.Record;
import com.risingwave.java.binding.rpc.MetaClient;
import java.util.Arrays;

/** Hello world! */
public class Demo {
    public static void main(String[] args) {
        String stateStore = System.getenv("STATE_STORE");
        int tableId = Integer.parseInt(System.getenv("TABLE_ID"));

        try (MetaClient metaClient = new MetaClient("127.0.0.1:5690");
                Iterator iter = new Iterator(metaClient, stateStore, tableId)) {
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
    }
}
