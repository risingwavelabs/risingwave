package com.risingwave.java.binding;

import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@BenchmarkMode(org.openjdk.jmh.annotations.Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
public class StreamchunkBenchmark {
    @Param({"100", "1000", "10000"})
    static int loopTime;

    String str;
    StreamChunkIterator iter;

    @Setup(Level.Invocation)
    public void setup() {
        str = "i i I f F B i";
        for (int i = 0; i < loopTime; i++) {
            String b = i % 2 == 0 ? "f" : "t";
            String n = i % 2 == 0 ? "." : "1";
            str += String.format("\n + %d %d %d %d.0 %d.0 %s %s", i, i, i, i, i, b, n);
        }
        iter = new StreamChunkIterator(str);
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
        int count = 0;
        while (true) {
            try (StreamChunkRow row = iter.next()) {
                if (row == null) {
                    break;
                }
                count += 1;
                getValue(row);
            }
        }
        if (count != loopTime) {
            throw new RuntimeException(
                    String.format("row count is %s, should be %s", count, loopTime));
        }
    }
}
