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

import java.io.IOException;
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A server that exposes user-defined functions over Apache Arrow Flight. */
public class UdfServer implements AutoCloseable {

    private FlightServer server;
    private UdfProducer producer;
    private static final Logger logger = LoggerFactory.getLogger(UdfServer.class);

    public UdfServer(String host, int port) {
        var location = Location.forGrpcInsecure(host, port);
        var allocator = new RootAllocator();
        this.producer = new UdfProducer(allocator);
        this.server = FlightServer.builder(allocator, location, this.producer).build();
    }

    /**
     * Add a user-defined function to the server.
     *
     * @param name the name of the function
     * @param udf the function to add
     * @throws IllegalArgumentException if a function with the same name already exists
     */
    public void addFunction(String name, UserDefinedFunction udf) throws IllegalArgumentException {
        logger.info("added function: " + name);
        this.producer.addFunction(name, udf);
    }

    /**
     * Start the server.
     *
     * @throws IOException if the server fails to start
     */
    public void start() throws IOException {
        this.server.start();
        logger.info("listening on " + this.server.getLocation().toSocketAddress());
    }

    /**
     * Get the port the server is listening on.
     *
     * @return the port number
     */
    public int getPort() {
        return this.server.getPort();
    }

    /**
     * Wait for the server to terminate.
     *
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public void awaitTermination() throws InterruptedException {
        this.server.awaitTermination();
    }

    /** Close the server. */
    public void close() throws InterruptedException {
        this.server.close();
    }
}
