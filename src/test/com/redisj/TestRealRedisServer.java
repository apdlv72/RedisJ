package com.redisj;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import redis.clients.jedis.Jedis;

public class TestRealRedisServer extends TestRedisServer {

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        PORT   = 6381;
        server = null;

        try {
            client = new Jedis("127.0.0.1", PORT, 60*1000);
            client.ping();
        }
        catch (Exception e) {
            fail("Failed to connect real redis server on port " + PORT);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() {
        client.close();
    }
}
