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
import org.apache.arrow.vector.types.pojo.Schema;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.reflect.Method;

/**
 * Base class for a batch-processing user-defined function.
 */
abstract class UserDefinedFunctionBatch {
    protected Schema inputSchema;
    protected Schema outputSchema;
    protected BufferAllocator allocator;

    /**
     * Get the input schema of the function.
     */
    Schema getInputSchema() {
        return inputSchema;
    }

    /**
     * Get the output schema of the function.
     */
    Schema getOutputSchema() {
        return outputSchema;
    }

    /**
     * Evaluate the function by processing a batch of input data.
     *
     * @param batch the input data batch to process
     * @return an iterator over the output data batches
     */
    abstract Iterator<VectorSchemaRoot> evalBatch(VectorSchemaRoot batch);
}

/**
 * Utility class for reflection.
 */
class Reflection {
    /**
     * Get the method named <code>eval</code>.
     */
    static Method getEvalMethod(UserDefinedFunction obj) {
        var methods = new ArrayList<Method>();
        for (Method method : obj.getClass().getDeclaredMethods()) {
            if (method.getName().equals("eval")) {
                methods.add(method);
            }
        }
        if (methods.size() != 1) {
            throw new IllegalArgumentException(
                    "Exactly one eval method must be defined for class " + obj.getClass().getName());
        }
        return methods.get(0);
    }

    /**
     * Get the method handle of the given method.
     */
    static MethodHandle getMethodHandle(Method method) {
        var lookup = MethodHandles.lookup();
        try {
            return lookup.unreflect(method);
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "The eval method must be public for class " + method.getDeclaringClass().getName());
        }
    }
}
