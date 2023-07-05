// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.risingwave.java.utils;

import static io.grpc.Status.INVALID_ARGUMENT;

import java.net.URI;
import java.net.URISyntaxException;

public class UrlParser {
    public static String parseLocationScheme(String location) {
        try {
            URI uri = new URI(location);
            String scheme = uri.getScheme();
            if (scheme == null) {
                throw INVALID_ARGUMENT
                        .withDescription("location should set scheme (e.g. s3a://)")
                        .asRuntimeException();
            }
            return scheme;
        } catch (URISyntaxException e) {
            throw INVALID_ARGUMENT
                    .withDescription(String.format("invalid location uri: %s", e.getMessage()))
                    .withCause(e)
                    .asRuntimeException();
        }
    }
}
