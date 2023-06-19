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

/**
 * Base interface for a user-defined scalar function. A user-defined scalar
 * function maps zero, one, or multiple scalar values to a new scalar value.
 * 
 * <p>
 * The behavior of a {@link ScalarFunction} can be defined by implementing a
 * custom evaluation method. An evaluation method must be declared publicly, not
 * static, and named <code>eval</code>. Multiple overloaded methods named
 * <code>eval</code> are not supported yet.
 * 
 * <p>
 * By default, input and output data types are automatically extracted using
 * reflection.
 * 
 * <p>
 * The following examples show how to specify a scalar function:
 *
 * <pre>
 * {@code
 * // a function that accepts two INT arguments and computes a sum
 * class SumFunction implements ScalarFunction {
 *     public Integer eval(Integer a, Integer b) {
 *         return a + b;
 *     }
 * }
 * 
 * // a function that returns a struct type
 * class StructFunction implements ScalarFunction {
 *     public static class KeyValue {
 *         public String key;
 *         public int value;
 *     }
 * 
 *     public KeyValue eval(int a) {
 *         KeyValue kv = new KeyValue();
 *         kv.key = a.toString();
 *         kv.value = a;
 *         return kv;
 *     }
 * }
 * }</pre>
 */
public interface ScalarFunction extends UserDefinedFunction {
}
