package com.risingwave.functions;

/**
 * Base interface for a user-defined scalar function. A user-defined scalar function
 * maps zero, one, or multiple scalar values to a new scalar value.
 * 
 * <p>
 * The behavior of a {@link ScalarFunction} can be defined by implementing a
 * custom evaluation method. An evaluation method must be declared publicly and
 * named <code>eval</code>. Multiple overloaded methods named <code>eval</code>
 * are not supported yet.
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
public abstract interface ScalarFunction extends UserDefinedFunction {
}
