package com.risingwave.java.binding;

import static com.risingwave.java.binding.Utils.validateRow;

import java.io.IOException;

public class StreamChunkDemo {

    public static void main(String[] args) throws IOException {
        byte[] payload = System.in.readAllBytes();
        try (StreamChunkIterator iter = new StreamChunkIterator(payload)) {
            int count = 0;
            while (true) {
                try (StreamChunkRow row = iter.next()) {
                    if (row == null) {
                        break;
                    }
                    count += 1;
                    validateRow(row);
                }
            }
            int expectedCount = 30000;
            if (count != expectedCount) {
                throw new RuntimeException(
                        String.format("row count is %s, should be %s", count, expectedCount));
            }
        }
    }
}
