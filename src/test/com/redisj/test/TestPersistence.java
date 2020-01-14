package com.redisj.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.redisj.RedisServer;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public class TestPersistence {

    protected static int PORT = 7379;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        server = new RedisServer(PORT)
                .withPersistence()
                ;
        boolean background = true;
        server.serveForEver(background);
        server.waitUntilListening();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        if (null!=server) {
            server.stop();
        }
    }

    @Before
    public void setUp() {
        if (null!=server) {
            server.flushAll();
        }
    }

    @Test
    public void testString() throws IOException {
        server.flushDb(0);
        server.set(0, "key", "value");
        server.persist(0);
    }

    @Test
    public void testHash() throws IOException {
        server.flushDb(0);
        String resp = server.hset(0, "key", "field", "value");
        assertEquals(":1\n", resp);
        server.persist(0);
    }

    protected static RedisServer server;
}