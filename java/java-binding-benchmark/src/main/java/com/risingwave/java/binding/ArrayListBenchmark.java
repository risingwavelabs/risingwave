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

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.*;

@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.MILLISECONDS)
@Fork(value = 1)
@BenchmarkMode(org.openjdk.jmh.annotations.Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(org.openjdk.jmh.annotations.Scope.Benchmark)
public class ArrayListBenchmark {
    @Param({"100", "1000", "10000"})
    static int loopTime;

    ArrayList<ArrayList<Object>> data = new ArrayList<>();

    public ArrayList<Object> getRow(int index) {
        short v1 = (short) index;
        int v2 = (int) index;
        long v3 = (long) index;
        float v4 = (float) index;
        double v5 = (double) index;
        boolean v6 = index % 3 == 0;
        Integer mayNull = null;
        ArrayList<Object> rowData = new ArrayList<>();
        rowData.add(v1);
        rowData.add(v2);
        rowData.add(v3);
        rowData.add(v4);
        rowData.add(v5);
        rowData.add(v6);
        rowData.add(mayNull);
        return rowData;
    }

    public void getValue(ArrayList<Object> rowData) {
        short value1 = (short) rowData.get(0);
        int value2 = (int) rowData.get(1);
        long value3 = (long) rowData.get(2);
        float value4 = (float) rowData.get(3);
        double value5 = (double) rowData.get(4);
        boolean value6 = (boolean) rowData.get(5);
        Integer mayNull = (Integer) rowData.get(6);
    }

    @Setup
    public void setup() {
        for (int i = 0; i < loopTime; i++) {
            data.add(getRow(i));
        }
    }

    @Benchmark
    public void arrayListTest() {
        for (int i = 0; i < loopTime; i++) {
            getValue(data.get(i));
        }
    }
}
