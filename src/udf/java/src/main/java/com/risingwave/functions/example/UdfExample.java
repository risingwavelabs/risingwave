package com.risingwave.functions.example;

import java.io.IOException;

import com.risingwave.functions.UdfServer;

public class UdfExample {
    public static void main(String[] args) throws IOException {
        try (var server = new UdfServer("0.0.0.0", 8815)) {
            server.addFunction("int42", new Int42());
            server.addFunction("gcd", new Gcd());
            server.addFunction("gcd3", new Gcd());

            server.start();
            server.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
