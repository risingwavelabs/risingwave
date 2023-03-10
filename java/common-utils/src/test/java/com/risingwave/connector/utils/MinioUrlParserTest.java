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

package com.risingwave.connector.utils;

import static org.junit.Assert.assertEquals;

import com.risingwave.java.utils.MinioUrlParser;
import org.junit.Assert;
import org.junit.Test;

public class MinioUrlParserTest {
    @Test
    public void testParseMinioUrl() {
        String url = "minio://minioadmin:minioadmin@127.0.0.1:9000/bucket";
        MinioUrlParser minioUrlParser = new MinioUrlParser(url);
        assertEquals("minioadmin", minioUrlParser.getKey());
        assertEquals("minioadmin", minioUrlParser.getSecret());
        assertEquals("127.0.0.1", minioUrlParser.getAddress());
        assertEquals("9000", minioUrlParser.getPort());
        assertEquals("http://127.0.0.1:9000", minioUrlParser.getEndpoint());
        assertEquals("bucket", minioUrlParser.getBucket());
    }

    private void assertWrong(String url) {
        boolean exceptionThrown = false;
        try {
            new MinioUrlParser(url);
        } catch (RuntimeException e) {
            exceptionThrown = true;
            Assert.assertTrue(
                    e.getMessage()
                            .toLowerCase()
                            .contains(
                                    "url for minio should be like minio://key:secret@address:port/bucket"));
        }
        if (!exceptionThrown) {
            Assert.fail(
                    "Expected exception not thrown: `url for minio should be like minio://key:secret@address:port/bucket`");
        }
    }

    @Test
    public void testParseWrongUrl() {
        assertWrong("/tmp/rw-sinknode");
        assertWrong("s3://bucket");
        assertWrong("minio://bucket");
        assertWrong("minio://minioadmin:minioadmin@127.0.0.1:9000");
    }
}
