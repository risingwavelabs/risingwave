package com.risingwave.binding;

import java.util.Arrays;

/**
 * Hello world!
 */
public class Main {
    public static void main(String[] args) {
        try (Iterator iter = new Iterator()) {
            while (true) {
                NextResult result = iter.next();
                if (result.isNone()) {
                    break;
                }
                System.out.printf("key %s, value %s%n", Arrays.toString(result.getKey()), Arrays.toString(result.getValue()));
            }
        }
    }
}
