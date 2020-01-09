package com.redisj;

import static org.junit.Assert.assertEquals;
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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public class TestRedisServer {

    protected static int PORT = 7379;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        server = new RedisServer(PORT);
        client = new Jedis("127.0.0.1", PORT, 60*1000);
        boolean background = true;
        server.serveForEver(background);
        server.waitUntilListening();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        client.close();
        if (null!=server) {
            server.stop();
        }
    }

    @Before
    public void setUp() {
        if (null!=server) {
            server.flush();
        }
    }

    @Test
    public void testPing() {
        String pong = client.ping();
        assertEquals("PONG", pong);
    }

    @Test
    public void testEcho() {
        String otto = client.echo("Otto");
        assertEquals("Otto", otto);
    }

    @Test
    public void testSave() {
        client.select(0);
        client.set("A", "a string\r\n");
        client.incr("B");

        client.select(1);
        client.lpush("L", "one");
        client.lpush("L", "two");
        client.lpush("L", "thre");
        client.incr("C");

        client.save();
    }

    @Test
    public void testClient() {
        // null
        @SuppressWarnings("unused")
        String name = client.clientGetname();

        // id=58 addr=127.0.0.1:64327 fd=7 name= age=6 idle=0 flags=N db=1 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=client
        @SuppressWarnings("unused")
        String list = client.clientList();

        // OK
        String rc = client.clientSetname("blabla");
        assertEquals("OK", rc);

        String name2 = client.clientGetname();
        assertEquals("blabla", name2);
    }

    @Test
    public void testSelect() {
        final String key       = "testSelect";
        final String expected0 = "aaa";
        final String expected1 = "bbb";

        String rc0 = client.select(0);
        assertEquals("OK", rc0);
        client.set(key, expected0);

        String rc1 = client.select(1);
        assertEquals("OK", rc1);
        client.set(key, expected1);

        client.select(0);
        String actual0 = client.get(key);
        client.select(1);
        String actual1 = client.get(key);

        assertEquals(expected0, actual0);
        assertEquals(expected1, actual1);

        client.select(0);
        client.del(key);
        client.select(1);
        client.del(key);

    }

    @Test
    public void testSetGet() {
        final String key      = "testSetGet";
        final String expected = "123";
        client.set(key, expected);
        String actual = client.get(key);
        assertEquals(expected, actual);
    }

    @Test
    public void testAppend() {
        final String key      = "testAppend";
        final String expected = "123";
        client.set(key, expected);
        Long length = client.append(key, "tail");
        assertEquals((Long)7L, length);
        String actual = client.get(key);
        assertEquals("123tail", actual);
    }

    @Test
    public void testMGet() {
        final String key0      = "testMGet0";
        final String key1      = "testMGet1";
        final String key2      = "testMGet2";
        final String expected0 = "012";
        final String expected1 = "123";
        final String expected2 = "234";
        client.set(key0, expected0);
        client.set(key1, expected1);
        client.set(key2, expected2);
        List<String> actual = client.mget(key0, key1, key2);
        assertEquals(3, actual.size());

        assertEquals(expected0, actual.get(0));
        assertEquals(expected1, actual.get(1));
        assertEquals(expected2, actual.get(2));
    }

    @Test
    public void testKeys() {

        String   rc = client.select(7);
        assertEquals("OK", rc);

        client.flushDB();

        client.mset(
                "x1", "one",
                "x2", "two",
                "y1", "one",
                "y2", "two",
                "y3", "tri"
                );
        Set<String> xxx = client.keys("x*");
        Set<String> yyy = client.keys("y*");
        Set<String> all = client.keys("*");
        assertEquals(2,  xxx.size());
        assertEquals(3,  yyy.size());
        assertEquals(5,  all.size());
    }

    @Test
    public void testType() {

        client.flushDB();

        String actualA = client.set("A", "123");
        String actualB = client.set("B", "321");
        Long   actualC = client.incrBy("C", 2L);
        Long   actualD = client.lpush("D", "x");
        assertEquals("OK", actualA);
        assertEquals("OK", actualB);
        assertEquals((Long)2L, actualC);
        assertEquals((Long)1L, actualD);

        String typeA = client.type("A");
        String typeB = client.type("B");
        String typeC = client.type("C");
        String typeD = client.type("D");
        assertEquals("string", typeA);
        assertEquals("string", typeB);
        assertEquals("string", typeC);
        assertEquals("list",   typeD);
    }

    @Test
    public void testDel() {
        final String key      = "testDel";
        final String expected = "1234";
        client.set(key, expected);
        String actual = client.get(key);
        assertEquals(expected, actual);

        client.del(key);
        String none = client.get(key);
        assertNull(none);
    }

    @Test
    public void testLlen() {
        final String key1 = "testLlen";

        client.lpush(key1, "1");
        client.lpush(key1, "2");
        client.lpush(key1, "3");
        Long actual1 = client.llen(key1);
        assertEquals((Long)3L, actual1);

        client.del(key1);
        Long actual2 = client.llen(key1);
        assertEquals((Long)0L, actual2);

        final String key2 = "not_a_list";

        try {
            client.set(key2, "no, it's not");
            @SuppressWarnings("unused")
            Long actual3 = client.llen(key2);
            fail("Should throw an exception");
        }
        catch (JedisDataException e) {
            String msg = e.getMessage();
            assertTrue(msg.equals("WRONGTYPE Operation against a key holding the wrong kind of value"));
        }
    }

    @Test
    public void testIncr() {

        final String key = "testIncr";
        client.del(key);

        assertNull(client.get(key));

        Long actual1 = client.incr(key);
        Long actual2 = client.incr(key);
        assertEquals((Long)1L, actual1);
        assertEquals((Long)2L, actual2);
    }

    @Test
    public void testIncrByFloat() {

        final String key = "testIncrByFloat";
        assertNull(client.get(key));
        Long   actual1 = client.incr(key);
        Double actual2 = client.incrByFloat(key, 0.5);
        assertEquals((Long)1L, actual1);
        assertEquals((Double)1.5, actual2);
    }

    @Test
    public void testLPop() {

        final String key = "testLPop1";
        final String expected1 = "one";
        final String expected2 = "two";

        client.lpush(key, expected2);
        client.lpush(key, expected1);

        String actual1 = client.lpop(key);
        String actual2 = client.lpop(key);
        assertEquals(expected1, actual1);
        assertEquals(expected2, actual2);
    }

    @Test
    public void testRPop() {

        final String key = "testRPop1";
        final String expected1 = "one";
        final String expected2 = "two";

        client.lpush(key, expected1);
        client.lpush(key, expected2);

        String actual1 = client.rpop(key);
        String actual2 = client.rpop(key);
        assertEquals(expected1, actual1);
        assertEquals(expected2, actual2);
    }

    @Test
    public void testBLPop() {

        final String keyA = "testBLPopA";
        final String keyB = "testBLPopB";
        final String expected1 = "one";
        final String expected2 = "two";

        client.lpush(keyA, expected2);
        client.lpush(keyA, expected1);
        client.lpush(keyB, expected2);
        client.lpush(keyB, expected1);

        List<String> actual1 = client.blpop(5, keyA, keyB);
        List<String> actual2 = client.blpop(5, keyA, keyB);
        List<String> actual3 = client.blpop(5, keyA, keyB);
        List<String> actual4 = client.blpop(5, keyA, keyB);

        assertEquals("testBLPopA", actual1.get(0));
        assertEquals("one", actual1.get(1));

        assertEquals("testBLPopA", actual2.get(0));
        assertEquals("two", actual2.get(1));

        assertEquals("testBLPopB", actual3.get(0));
        assertEquals("one", actual3.get(1));

        assertEquals("testBLPopB", actual4.get(0));
        assertEquals("two", actual4.get(1));

        List<String> actual5 = client.blpop(1, keyA, keyB);
        assertNull(actual5);
    }

    @Test
    public void testHash() {
        String key = "struct";

        client.del(key);

        client.set(key, "string");
        try {
            client.hexists(key, "bla");
        }
        catch (Exception e) {
            String actual = e.getMessage();
            assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", actual);
        }

        client.del(key);
        boolean e0 = client.hexists(key, "a");
        assertEquals(false, e0);

        long rc1 = client.hsetnx(key, "a", "1");
        assertEquals(1, rc1);

        boolean e1 = client.hexists(key, "a");
        assertEquals(true, e1);

        long rc2 = client.hsetnx(key, "a", "1");
        assertEquals(0, rc2);

        long rc3 = client.hset(key, "b", "2");
        assertEquals(1, rc3);

        client.hincrByFloat(key, "a", .5);
        client.hincrBy(key, "b", 1);

        Map<String, String> all = client.hgetAll(key);
        assertEquals(2, all.size());
        assertEquals("1.5", all.get("a"));
        assertEquals("3", all.get("b"));

        String val1 = client.hget(key, "a");
        assertEquals("1.5", val1);

        String val2 = client.hget(key, "b");
        assertEquals("3", val2);

        Set<String> keys = client.hkeys(key);
        assertEquals(2, keys.size());

        long len = client.hlen(key);
        assertEquals(2, len);

        Map<String, String> map = new HashMap<String, String>();
        map.put("c", "X");
        map.put("d", "Y");
        String rc = client.hmset(key, map);
        assertNotNull(rc);

        List<String> list = client.hmget(key, "a", "b", "c", "d", "missing");
        assertEquals(5, list.size());
        assertEquals("1.5", list.get(0));
        assertEquals("3",   list.get(1));
        assertEquals("X",   list.get(2));
        assertEquals("Y",   list.get(3));
        assertEquals(null,  list.get(4));

        try {
            long slen = client.hstrlen(key, "a");
            assertEquals(3, slen);
        }
        catch (JedisDataException e) {
            System.err.println("WARNING: Redis server does not support HSTRLEN");
        }

        List<String> vals = client.hvals(key);
        assertEquals(4, vals.size());
        assertTrue(vals.contains("1.5"));
        assertTrue(vals.contains("3"));
        assertTrue(vals.contains("Y"));
        assertTrue(vals.contains("X"));

        try {
            rc = client.get(key);
            fail("Exception expected");
        }
        catch (Exception e) {
            String actual = e.getMessage();
            assertEquals("WRONGTYPE Operation against a key holding the wrong kind of value", actual);
        }

    }

    protected static RedisServer server;
    protected static Jedis client;
}