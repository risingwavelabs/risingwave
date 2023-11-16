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
