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

public class MinioUrlParser {
    private final String key;
    private final String secret;
    private final String address;
    private final String port;
    private final String endpoint;
    private final String bucket;

    public MinioUrlParser(String url) {
        // url must be in the form of
        // minio://key:secret@address:port/bucket
        String info = url.substring(url.indexOf("//") + 2);
        String[] infoList = info.split("/|@|:", 5);
        if (infoList.length != 5) {
            throw INVALID_ARGUMENT
                    .withDescription(
                            "url for minio should be like minio://key:secret@address:port/bucket")
                    .asRuntimeException();
        }
        this.key = infoList[0];
        this.secret = infoList[1];
        this.address = infoList[2];
        this.port = infoList[3];
        this.endpoint = "http://" + infoList[2] + ":" + infoList[3];
        this.bucket = infoList[4];
    }

    public String getKey() {
        return key;
    }

    public String getSecret() {
        return secret;
    }

    public String getAddress() {
        return address;
    }

    public String getPort() {
        return port;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getBucket() {
        return bucket;
    }
}
