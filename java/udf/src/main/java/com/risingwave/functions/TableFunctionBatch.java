package com.risingwave.functions;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Batch-processing wrapper over a user-defined table function.
 */
class TableFunctionBatch extends UserDefinedFunctionBatch {
    TableFunction<?> function;
    Method method;
    Function<Object, Object>[] processInputs;
    int chunk_size = 1024;

    TableFunctionBatch(TableFunction<?> function, BufferAllocator allocator) {
        this.function = function;
        this.allocator = allocator;
        this.method = Reflection.getEvalMethod(function);
        this.inputSchema = TypeUtils.methodToInputSchema(this.method);
        this.outputSchema = TypeUtils.tableFunctionToOutputSchema(function.getClass());
        this.processInputs = TypeUtils.methodToProcessInputs(this.method);
    }

    @Override
    Iterator<VectorSchemaRoot> evalBatch(VectorSchemaRoot batch) {
        // TODO: incremental compute and output
        // due to the lack of generator support in Java, we can not yield from a
        // function call.
        var outputs = new ArrayList<VectorSchemaRoot>();
        var row = new Object[batch.getSchema().getFields().size()];
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
            for (int j = 0; j < row.length; j++) {
                var val = batch.getVector(j).getObject(i);
                row[j] = this.processInputs[j].apply(val);
            }
            // call function
            var size_before = this.function.size();
            try {
                this.method.invoke(this.function, row);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            var size_after = this.function.size();
            // add indexes
            for (int j = size_before; j < size_after; j++) {
                indexes.add(i);
            }
            // check if we need to flush
            if (size_after >= this.chunk_size) {
                buildChunk.run();
            }
        }
        if (this.function.size() > 0) {
            buildChunk.run();
        }
        return outputs.iterator();
    }
}
