package com.redisj;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

public class TestRedisServer {

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        server2 = new RedisServer(PORT);
        client = new Jedis("127.0.0.1", 7379, 60*1000);
        boolean background = true;
        server2.serveForEver(background);
        server2.waitUntilListening();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        client.close();
        server2.stop();
    }

    @Before
    public void setUp() {
        server2.flushAll();
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

    private static final int PORT = 7379;
    private static RedisServer server2;
    private static Jedis client;
    //private Database db;

}