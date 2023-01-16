package com.risingwave.java;

import com.risingwave.java.binding.Iterator;
import com.risingwave.java.binding.KeyedRow;
import java.util.Arrays;

/** Hello world! */
public class Demo {
    public static void main(String[] args) {
        try (Iterator iter = new Iterator()) {
            while (true) {
                try (KeyedRow row = iter.next()) {
                    if (row == null) {
                        break;
                    }
                    System.out.printf(
                            "key %s, id: %d, name: %s, is null: %s%n",
                            Arrays.toString(row.getKey()),
                            row.getLong(0),
                            row.getString(1),
                            row.isNull(2));
                }
            }
        }
    }
}
