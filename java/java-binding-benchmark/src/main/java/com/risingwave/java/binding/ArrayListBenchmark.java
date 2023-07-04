package com.risingwave.java.binding;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@BenchmarkMode(org.openjdk.jmh.annotations.Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
public class ArrayListBenchmark {
    @Param({"1000", "10000", "100000"})
    static int loopTime;

    ArrayList<ArrayList<Object>> data = new ArrayList<>();

    public ArrayList<Object> getRow(int index) {
        short v1 = (short) index;
        int v2 = (int) index;
        long v3 = (long) index;
        float v4 = (float) index;
        double v5 = (double) index;
        boolean v6 = index % 3 == 0;
        String v7 =
                "'"
                        + new String(new char[(index % 10) + 1])
                                .replace("\0", String.valueOf(index))
                        + "'";
        String v8 = "to_timestamp(" + index + ")";
        int v9 = index;
        Integer mayNull = null;
        ArrayList<Object> rowData = new ArrayList<>();
        rowData.add(v1);
        rowData.add(v2);
        rowData.add(v3);
        rowData.add(v4);
        rowData.add(v5);
        rowData.add(v6);
        rowData.add(v7);
        rowData.add(v8);
        rowData.add(v9);
        rowData.add(mayNull);
        return rowData;
    }

    public double processRowData(ArrayList<Object> rowData) {
        short value1 = (short) rowData.get(0);
        int value2 = (int) rowData.get(1);
        long value3 = (long) rowData.get(2);
        float value4 = (float) rowData.get(3);
        double value5 = (double) rowData.get(4);
        boolean value6 = (boolean) rowData.get(5);
        // String value7 = (String) rowData.get(6);
        // String value8 = (String) rowData.get(7);
        // int value9 = (int) rowData.get(8);
        Integer mayNull = (Integer) rowData.get(9);
        return value1 + value2 + value3 + value4 + value5;
    }

    @Setup
    public void setup() {
        for (int i = 0; i < loopTime; i++) {
            data.add(getRow(i));
        }
    }

    @Benchmark
    public void arrayListTest() {
        // Call your function here
        for (int i = 0; i < loopTime; i++) {
            processRowData(data.get(i));
        }
    }
}
