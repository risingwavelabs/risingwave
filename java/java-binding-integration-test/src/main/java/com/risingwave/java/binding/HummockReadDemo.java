// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.java.binding;

import static com.risingwave.java.binding.Utils.validateRow;

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
public class HummockReadDemo {
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

            try (HummockIterator iter = new HummockIterator(readPlan)) {
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
}
