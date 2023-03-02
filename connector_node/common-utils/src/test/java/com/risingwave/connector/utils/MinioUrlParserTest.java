package com.risingwave.connector.utils;

import static org.junit.Assert.assertEquals;

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
