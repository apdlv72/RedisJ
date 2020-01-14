package com.redisj.test;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.redisj.RedisServer;
import com.redisj.RedisServer.Database;

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
        Database db = server.select(0).flushDb();
        db.set("key", "value");
        server.persist(0);
    }

    @Test
    public void testHash() throws IOException {
        Database db = server.select(0).flushDb();
        int resp = db.hset("key", "field", "value");
        assertEquals(1, resp);
        server.persist(0);
    }

    @Test
    public void testSet() throws IOException {
        Database db = server.select(0).flushDb();
        int resp = db.sadd("key", "member1", "member2");
        assertEquals(2, resp);
        server.persist(0);
    }

    protected static RedisServer server;
}