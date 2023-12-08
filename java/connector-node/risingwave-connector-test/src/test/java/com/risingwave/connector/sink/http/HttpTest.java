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

package com.risingwave.connector.sink.http;

import static java.lang.Thread.sleep;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.risingwave.connector.api.ColumnDesc;
import com.risingwave.connector.api.TableSchema;
import com.risingwave.connector.api.sink.ArraySinkRow;
import com.risingwave.connector.api.sink.SinkRow;
import com.risingwave.connector.api.sink.SinkWriter;
import com.risingwave.mock.flink.http.HttpFlinkMockSinkFactory;
import com.risingwave.mock.flink.runtime.FlinkDynamicAdapterFactory;
import com.risingwave.proto.Data;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.Executors;
import org.junit.Test;

public class HttpTest {
    MyHttpHandler mockHttpClient() throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(8083), 0);

        MyHttpHandler myHttpHandler = new MyHttpHandler();

        httpServer.createContext("/test", myHttpHandler);

        httpServer.setExecutor(Executors.newFixedThreadPool(1));

        new Runnable() {
            @Override
            public void run() {
                httpServer.start();
            }
        }.run();
        return myHttpHandler;
    }

    class MyHttpHandler implements HttpHandler {
        String resultString;
        volatile boolean flag;

        public MyHttpHandler() {
            flag = false;
        }

        public void restartFlag() {
            this.flag = false;
        }

        public void waitHandle() throws InterruptedException {
            while (!flag) {
                sleep(500);
            }
        }

        @Override
        public void handle(HttpExchange httpExchange) throws IOException {
            Scanner scanner =
                    new Scanner(httpExchange.getRequestBody(), "UTF-8").useDelimiter("\\A");
            String resultString = scanner.hasNext() ? scanner.next() : "";

            httpExchange.sendResponseHeaders(200, 0);
            OutputStream outputStream = httpExchange.getResponseBody();
            String response = "Ok!";
            outputStream.write(response.getBytes());
            outputStream.close();

            this.resultString = resultString;
            this.flag = true;
        }

        public String getResultString() {
            return resultString;
        }
    }

    void checkList(String resultString, ArrayList<SinkRow> arrayList, TableSchema tableSchema)
            throws JsonProcessingException {
        List<ColumnDesc> columnDescs = tableSchema.getColumnDescs();

        for (int i = 0; i < arrayList.size(); i++) {
            SinkRow sinkRow = arrayList.get(i);
            TreeMap<String, Object> map1 = new TreeMap<>();
            for (int j = 0; j < sinkRow.size(); j++) {
                map1.put(columnDescs.get(j).getName(), sinkRow.get(j));
            }
            ObjectMapper objectMapper = new ObjectMapper();
            String s = objectMapper.writeValueAsString(map1);
            assert (resultString.contains(s));
        }
    }

    @Test
    public void testHttpSinkWrite() throws IOException, InterruptedException {
        MyHttpHandler httpHandler = mockHttpClient();
        TableSchema tableSchema =
                new TableSchema(
                        Lists.newArrayList("id", "name"),
                        Lists.newArrayList(
                                Data.DataType.newBuilder()
                                        .setTypeName(Data.DataType.TypeName.INT32)
                                        .build(),
                                Data.DataType.newBuilder()
                                        .setTypeName(Data.DataType.TypeName.VARCHAR)
                                        .build()),
                        Lists.newArrayList("id", "name"));

        FlinkDynamicAdapterFactory flinkDynamicAdapterFactory =
                new FlinkDynamicAdapterFactory(new HttpFlinkMockSinkFactory());
        HashMap<String, String> config = new HashMap<>();
        config.put("connector", "http");
        config.put("url", "http://localhost:8083/test");
        config.put("format", "json");
        config.put("type", "append-only");
        config.put("force_append_only", "true");
        config.put("primary_key", "id");

        SinkWriter writer = flinkDynamicAdapterFactory.createWriter(tableSchema, config);

        ArrayList<SinkRow> sinkRows = new ArrayList<>();
        sinkRows.add(new ArraySinkRow(Data.Op.INSERT, 1, "Alice"));
        sinkRows.add(new ArraySinkRow(Data.Op.INSERT, 2, "xxx"));

        writer.write(sinkRows);
        writer.barrier(true);
        httpHandler.waitHandle();
        httpHandler.restartFlag();

        checkList(httpHandler.getResultString(), sinkRows, tableSchema);

        ArrayList<SinkRow> sinkRows2 = new ArrayList<>();
        sinkRows2.add(new ArraySinkRow(Data.Op.INSERT, 3, "aaa"));
        sinkRows2.add(new ArraySinkRow(Data.Op.INSERT, 4, "bbb"));

        writer.write(sinkRows2);
        writer.barrier(true);
        httpHandler.waitHandle();

        checkList(httpHandler.getResultString(), sinkRows2, tableSchema);
    }
}
