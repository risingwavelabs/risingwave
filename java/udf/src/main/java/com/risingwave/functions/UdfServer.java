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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A server that exposes user-defined functions over Apache Arrow Flight.
 */
public class UdfServer implements AutoCloseable {

    private FlightServer server;
    private UdfProducer producer;
    private static final Logger logger = LoggerFactory.getLogger(UdfServer.class);

    public UdfServer(String host, int port) {
        var location = Location.forGrpcInsecure(host, port);
        var allocator = new RootAllocator();
        this.producer = new UdfProducer(allocator);
        this.server = FlightServer.builder(
                allocator,
                location,
                this.producer).build();
    }

    /**
     * Add a user-defined function to the server.
     * 
     * @param name the name of the function
     * @param udf  the function to add
     * @throws IllegalArgumentException if a function with the same name already
     *                                  exists
     */
    public void addFunction(String name, UserDefinedFunction udf) throws IllegalArgumentException {
        logger.info("added function: " + name);
        this.producer.addFunction(name, udf);
    }

    /**
     * Start the server.
     */
    public void start() throws IOException {
        this.server.start();
        logger.info("listening on " + this.server.getLocation().toSocketAddress());
    }

    /**
     * Get the port the server is listening on.
     */
    public int getPort() {
        return this.server.getPort();
    }

    /**
     * Wait for the server to terminate.
     */
    public void awaitTermination() throws InterruptedException {
        this.server.awaitTermination();
    }

    /**
     * Close the server.
     */
    public void close() throws InterruptedException {
        this.server.close();
    }
}

class UdfProducer extends NoOpFlightProducer {

    private BufferAllocator allocator;
    private HashMap<String, UserDefinedFunctionBatch> functions = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(UdfServer.class);

    UdfProducer(BufferAllocator allocator) {
        this.allocator = allocator;
    }

    void addFunction(String name, UserDefinedFunction function) throws IllegalArgumentException {
        UserDefinedFunctionBatch udf;
        if (function instanceof ScalarFunction) {
            udf = new ScalarFunctionBatch((ScalarFunction) function, this.allocator);
        } else if (function instanceof TableFunction) {
            udf = new TableFunctionBatch((TableFunction<?>) function, this.allocator);
        } else {
            throw new IllegalArgumentException("Unknown function type: " + function.getClass().getName());
        }
        if (functions.containsKey(name)) {
            throw new IllegalArgumentException("Function already exists: " + name);
        }
        functions.put(name, udf);
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        try {
            var functionName = descriptor.getPath().get(0);
            var udf = functions.get(functionName);
            if (udf == null) {
                throw new IllegalArgumentException("Unknown function: " + functionName);
            }
            var fields = new ArrayList<Field>();
            fields.addAll(udf.getInputSchema().getFields());
            fields.addAll(udf.getOutputSchema().getFields());
            var fullSchema = new Schema(fields);
            var input_len = udf.getInputSchema().getFields().size();

            return new FlightInfo(fullSchema, descriptor, Collections.emptyList(), 0, input_len);
        } catch (Exception e) {
            logger.error("Error occurred during getFlightInfo", e);
            throw e;
        }
    }

    @Override
    public void doExchange(CallContext context, FlightStream reader, ServerStreamListener writer) {
        try {
            var functionName = reader.getDescriptor().getPath().get(0);
            logger.debug("call function: " + functionName);

            var udf = this.functions.get(functionName);
            try (var root = VectorSchemaRoot.create(udf.getOutputSchema(), this.allocator)) {
                var loader = new VectorLoader(root);
                writer.start(root);
                while (reader.next()) {
                    var outputBatches = udf.evalBatch(reader.getRoot());
                    while (outputBatches.hasNext()) {
                        var outputRoot = outputBatches.next();
                        var unloader = new VectorUnloader(outputRoot);
                        loader.load(unloader.getRecordBatch());
                        writer.putNext();
                    }
                }
                writer.completed();
            }
        } catch (Exception e) {
            logger.error("Error occurred during UDF execution", e);
            writer.error(e);
        }
    }
}
