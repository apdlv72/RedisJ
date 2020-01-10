package com.redisj;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

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

    @Override
    @Before
    public void setUp() {
        if (null==client) {
            client = new Jedis("127.0.0.1", PORT, 60*1000);
        }
    }

    @AfterClass
    public static void tearDownAfterClass() {
        client.close();
    }

    @Test
    public void testAuth() {
        try {
            String rc = client.auth("password");
            assertEquals("OK", rc);
        }
        catch (JedisDataException e) {
            String msg = e.getMessage();
            assertEquals("ERR Client sent AUTH, but no password is set", msg);
        }
        client.close();
        client = null;
    }

    @Test
    public void testQuit() {
        String rc = client.quit();
        assertEquals("OK", rc);
    }

    @Test
    public void testSwapDb() {
        try {
            String rc = client.swapDB(0, 1);
            assertEquals("OK", rc);
        }
        catch (JedisDataException e) {
            String msg = e.getMessage();
            assertEquals("ERR unknown command 'SWAPDB'", msg);
        }
    }

}
