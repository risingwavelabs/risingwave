/*
 * Copyright 2024 RisingWave Labs
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

package com.risingwave.connector.source;

import com.risingwave.connector.ConnectorServiceImpl;
import com.risingwave.proto.ConnectorServiceProto;
import io.grpc.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbSourceTest {
    private static final Logger LOG = LoggerFactory.getLogger(MongoDbSourceTest.class.getName());

    public static Server connectorService =
            ServerBuilder.forPort(SourceTestClient.DEFAULT_PORT)
                    .addService(new ConnectorServiceImpl())
                    .build();

    public static SourceTestClient testClient =
            new SourceTestClient(
                    Grpc.newChannelBuilder(
                                    "localhost:" + SourceTestClient.DEFAULT_PORT,
                                    InsecureChannelCredentials.create())
                            .build());

    @BeforeClass
    public static void init() {
        try {
            connectorService.start();
        } catch (Exception e) {
            LOG.error("failed to start connector service", e);
            Assert.fail();
        }
    }

    @AfterClass
    public static void cleanup() {
        connectorService.shutdown();
    }

    // manually test
    @Test
    @Ignore
    public void testSnapshotLoad() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("mongodb.url", "mongodb://localhost:27017/?replicaSet=rs0");
        props.put("collection.name", "dev.*");
        Iterator<ConnectorServiceProto.GetEventStreamResponse> eventStream =
                testClient.getEventStream(ConnectorServiceProto.SourceType.MONGODB, 3001, props);
        Callable<Integer> countTask =
                () -> {
                    int count = 0;
                    while (eventStream.hasNext()) {
                        List<ConnectorServiceProto.CdcMessage> messages =
                                eventStream.next().getEventsList();
                        for (ConnectorServiceProto.CdcMessage msg : messages) {
                            if (!msg.getPayload().isBlank()) {
                                count++;
                            }
                            // Only read 10 messages
                            if (count >= 10) {
                                return count;
                            }
                        }
                    }
                    return count;
                };

        var pool = Executors.newFixedThreadPool(1);
        var result = pool.submit(countTask);
        Assert.assertEquals(10, result.get().intValue());
    }
}
