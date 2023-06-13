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
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Batch-processing wrapper over a user-defined scalar function.
 */
class ScalarFunctionBatch extends UserDefinedFunctionBatch {
    ScalarFunction function;
    MethodHandle methodHandle;
    Function<Object, Object>[] processInputs;

    ScalarFunctionBatch(ScalarFunction function, BufferAllocator allocator) {
        this.function = function;
        this.allocator = allocator;
        var method = Reflection.getEvalMethod(function);
        this.methodHandle = Reflection.getMethodHandle(method);
        this.inputSchema = TypeUtils.methodToInputSchema(method);
        this.outputSchema = TypeUtils.methodToOutputSchema(method);
        this.processInputs = TypeUtils.methodToProcessInputs(method);
    }

    @Override
    Iterator<VectorSchemaRoot> evalBatch(VectorSchemaRoot batch) {
        var row = new Object[batch.getSchema().getFields().size() + 1];
        row[0] = this.function;
        var outputValues = new Object[batch.getRowCount()];
        for (int i = 0; i < batch.getRowCount(); i++) {
            for (int j = 0; j < row.length - 1; j++) {
                var val = batch.getVector(j).getObject(i);
                row[j + 1] = this.processInputs[j].apply(val);
            }
            try {
                outputValues[i] = this.methodHandle.invokeWithArguments(row);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        var outputVector = TypeUtils.createVector(this.outputSchema.getFields().get(0), this.allocator, outputValues);
        var outputBatch = VectorSchemaRoot.of(outputVector);
        return Collections.singleton(outputBatch).iterator();
    }
}
