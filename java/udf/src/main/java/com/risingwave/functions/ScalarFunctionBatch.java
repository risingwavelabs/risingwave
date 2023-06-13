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
        var row = new Object[batch.getSchema().getFields().size()];
        var outputValues = new Object[batch.getRowCount()];
        for (int i = 0; i < batch.getRowCount(); i++) {
            for (int j = 0; j < row.length; j++) {
                var val = batch.getVector(j).getObject(i);
                row[j] = this.processInputs[j].apply(val);
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
