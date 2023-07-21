package com.risingwave.java.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ObjectSerde {
    public static byte[] serializeObject(Serializable obj) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            try {
                ObjectOutputStream stream = new ObjectOutputStream(output);
                stream.writeObject(obj);
                stream.flush();
                return output.toByteArray();
            } finally {
                output.close();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Object deserializeObject(byte[] payload) {
        try {
            try (ByteArrayInputStream input = new ByteArrayInputStream(payload)) {
                ObjectInputStream stream = new ObjectInputStream(input);
                return stream.readObject();
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
