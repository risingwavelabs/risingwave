package com.risingwave.functions.example;

import com.risingwave.functions.ScalarFunction;

public class Int42 extends ScalarFunction {
    public static int eval() {
        return 42;
    }
}