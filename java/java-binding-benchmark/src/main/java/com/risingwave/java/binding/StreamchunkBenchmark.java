/*
 * Copyright 2023 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.risingwave.java.binding;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize = 10)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.MILLISECONDS, batchSize = 10)
@Fork(value = 1)
@BenchmarkMode(org.openjdk.jmh.annotations.Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
public class StreamchunkBenchmark {
    @Param({"100", "1000", "10000"})
    int loopTime;

    StreamChunk chunk;

    @Setup(Level.Iteration)
    public void setup() {
        String str = "i i I f F B i";
        for (int i = 0; i < loopTime; i++) {
            String b = i % 2 == 0 ? "f" : "t";
            String n = i % 2 == 0 ? "." : "1";
            str += String.format("\n + %d %d %d %d.0 %d.0 %s %s", i, i, i, i, i, b, n);
        }
        chunk = StreamChunk.fromPretty(str);
    }

    public void getValue(StreamChunkRow row) {
        short value1 = (short) row.getInt(0);
        int value2 = (int) row.getInt(1);
        Long value3 = (long) row.getLong(2);
        float value4 = (float) row.getFloat(3);
        double value5 = (double) row.getDouble(4);
        boolean value6 = (boolean) row.getBoolean(5);
        boolean mayNull = row.isNull(6);
    }

    @Benchmark
    public void streamchunkTest() {
        var iter = new StreamChunkIterator(chunk);
        int count = 0;
        while (true) {
            StreamChunkRow row = iter.next();
            if (row == null) {
                break;
            }
            count += 1;
            getValue(row);
        }
        if (count != loopTime) {
            throw new RuntimeException(
                    String.format("row count is %s, should be %s", count, loopTime));
        }
    }
}
