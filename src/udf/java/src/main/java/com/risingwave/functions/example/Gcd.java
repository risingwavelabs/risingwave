package com.risingwave.functions.example;

import com.risingwave.functions.ScalarFunction;

public class Gcd extends ScalarFunction {
    public static int eval(int a, int b) {
        while (b != 0) {
            int temp = b;
            b = a % b;
            a = temp;
        }
        return a;
    }

    // TODO: support multiple eval functions
    // public static int eval(int a, int b, int c) {
    // return eval(eval(a, b), c);
    // }
}