package com.risingwave.binding;

import java.util.Arrays;

/**
 * Hello world!
 */
public class Main {
    public static void main(String[] args) {
        try (Iterator iter = new Iterator()) {
            while (true) {
                try (Record record = iter.next()) {
                    if (record == null) {
                        break;
                    }
                    System.out.printf("key %s, value %s%n", Arrays.toString(record.getKey()), Arrays.toString(record.getValue()));
                }
            }
        }
    }
}
