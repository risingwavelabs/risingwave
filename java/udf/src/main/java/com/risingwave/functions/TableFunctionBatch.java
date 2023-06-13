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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Batch-processing wrapper over a user-defined table function.
 */
class TableFunctionBatch extends UserDefinedFunctionBatch {
    TableFunction<?> function;
    MethodHandle methodHandle;
    Function<Object, Object>[] processInputs;
    int chunkSize = 1024;

    TableFunctionBatch(TableFunction<?> function, BufferAllocator allocator) {
        this.function = function;
        this.allocator = allocator;
        var method = Reflection.getEvalMethod(function);
        this.methodHandle = Reflection.getMethodHandle(method);
        this.inputSchema = TypeUtils.methodToInputSchema(method);
        this.outputSchema = TypeUtils.tableFunctionToOutputSchema(function.getClass());
        this.processInputs = TypeUtils.methodToProcessInputs(method);
    }

    @Override
    Iterator<VectorSchemaRoot> evalBatch(VectorSchemaRoot batch) {
        // TODO: incremental compute and output
        // due to the lack of generator support in Java, we can not yield from a
        // function call.
        var outputs = new ArrayList<VectorSchemaRoot>();
        var row = new Object[batch.getSchema().getFields().size() + 1];
        row[0] = this.function;
        var indexes = new ArrayList<Integer>();
        Runnable buildChunk = () -> {
            var indexVector = TypeUtils.createVector(this.outputSchema.getFields().get(0), this.allocator,
                    indexes.toArray());
            indexes.clear();
            var valueVector = TypeUtils.createVector(this.outputSchema.getFields().get(1), this.allocator,
                    this.function.take());
            var outputBatch = VectorSchemaRoot.of(indexVector, valueVector);
            outputs.add(outputBatch);
        };
        for (int i = 0; i < batch.getRowCount(); i++) {
            // prepare input row
            for (int j = 0; j < row.length - 1; j++) {
                var val = batch.getVector(j).getObject(i);
                row[j + 1] = this.processInputs[j].apply(val);
            }
            // call function
            var sizeBefore = this.function.size();
            try {
                this.methodHandle.invokeWithArguments(row);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            var sizeAfter = this.function.size();
            // add indexes
            for (int j = sizeBefore; j < sizeAfter; j++) {
                indexes.add(i);
            }
            // check if we need to flush
            if (sizeAfter >= this.chunkSize) {
                buildChunk.run();
            }
        }
        if (this.function.size() > 0) {
            buildChunk.run();
        }
        return outputs.iterator();
    }
}
