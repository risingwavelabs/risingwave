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

package com.risingwave.functions;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UdfClient implements AutoCloseable {

    private FlightClient client;
    private static final Logger logger = LoggerFactory.getLogger(UdfClient.class);

    public UdfClient(String host, int port) {
        var allocator = new RootAllocator();
        var location = Location.forGrpcInsecure(host, port);
        this.client = FlightClient.builder(allocator, location).build();
    }

    public void close() throws InterruptedException {
        this.client.close();
    }

    public FlightInfo getFlightInfo(String functionName) {
        var descriptor = FlightDescriptor.command(functionName.getBytes());
        return client.getInfo(descriptor);
    }

    public FlightStream call(String functionName, VectorSchemaRoot root) {
        var descriptor = FlightDescriptor.path(functionName);
        var readerWriter = client.doExchange(descriptor);
        var writer = readerWriter.getWriter();
        var reader = readerWriter.getReader();

        writer.start(root);
        writer.putNext();
        writer.completed();
        return reader;
    }
}
