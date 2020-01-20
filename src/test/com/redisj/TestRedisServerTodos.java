package com.redisj;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.BitOP;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ListPosition;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.Slowlog;

public class TestRedisServerTodos {

    private static final String OK = "OK";
    protected static int PORT = 7379;

    @BeforeClass
    public static void setUpBeforeClass() throws IOException {
        server = new RedisServer(PORT);
        client = null; //new Jedis("127.0.0.1", PORT, 60*1000);
        boolean background = true;
        server.serveForEver(background);
        server.waitUntilListening();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        if (null!=server) {
            server.stop();
        }
        if (null!=client) {
            client.close();
        }
    }

    @Before
    public void setUp() {
        if (null!=server) {
            server.flushAll();
        }
        client = new Jedis("127.0.0.1", PORT, 60*1000);
        client.del(key);
        client.del(key1);
        client.del(key2);
        client.del(key3);
        client.set(key,  value);
    }

    @After
    public void tearDown() {
        client.close();
        client = null;
    }

    @Test
    public void testAppend() throws IOException {
        Object actual = client.append(key, "value");
        Object expect = 5L;
        assertEquals(expect, actual);
    }

    @Test
    public void testAuth() throws IOException {
        try {
        Object actual = client.auth("passwd");
        Object expect = null;
        assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testBgrewriteaof() throws IOException {
        Object actual = client.bgrewriteaof();
        assertNotNull(actual);
    }

    @Test
    public void testBgsave() throws IOException {
//        Object actual = client.bgsave();
//        Object expect = null;
//        assertEquals(expect, actual);
    }

    @Test
    public void testBitcount() throws IOException {
        client.set(key, value);
        Object actual = client.bitcount(key);
        Object expect = 21L;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testBitfield() throws IOException {
//        Object actual = client.bitfield(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testBitop() throws IOException {
        BitOP op = BitOP.AND;
        Object actual = client.bitop(op, "dest", "src1", "src2");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testBitpos() throws IOException {
        Object actual = client.bitpos(key, true);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testBlpop() throws IOException {
        Object actual = client.blpop(key, "1");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testBrpop() throws IOException {
        Object actual = client.brpop(key, "1");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testBrpoplpush() throws IOException {
        Object actual = client.brpoplpush("source", "dest", 1);
        Object expect = null;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testBzpopmin() throws IOException {
//        Object actual = client.bzpopmin(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testBzpopmax() throws IOException {
//        Object actual = client.bzpopmax(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testClientGetname() throws IOException {
        Object actual = client.clientGetname();
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testClientList() throws IOException {
        Object actual = client.clientList();
        assertNotNull(actual);
    }

    @Test
    public void testClientSetName() throws IOException {
        Object actual = client.clientSetname("name");
        Object expect = OK;
        assertEquals(expect, actual);
    }

    public void testClientKillIpPort() throws IOException {
        Object actual = client.clientKill("ip", 0);
        Object expect = null;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testCluster() throws IOException {
//        Object actual = client.cluster(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testCommand() throws IOException {
//        Object actual = client.command(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testConfig() throws IOException {
        Object actual = client.configGet("*");
        assertNotNull(actual);
    }

    @Test
    public void testDbsize() throws IOException {
        client.flushDB();
        Object actual = client.dbSize();
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testDebugSEGFAULT() throws IOException {
        Object actual = client.debug(DebugParams.RELOAD());
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testDecr() throws IOException {
        Object actual = client.decr(key);
        Object expect = -1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testDecrby() throws IOException {
        Object actual = client.decrBy(key, 1);
        Object expect = -1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testDel() throws IOException {
        client.del(key1, key2, key3);
        Object actual = client.del(key1, key2, key3);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testDiscard() throws IOException {
//        Object actual = client.discard(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testDump() throws IOException {
        Object actual = client.dump(key);
        Object expect = null;
        assertNotNull(actual);
    }

    @Test
    public void testEcho() throws IOException {
        Object actual = client.echo("msg");
        Object expect = "msg";
        assertEquals(expect, actual);
    }

//    @Test
//    public void testEval() throws IOException {
//        Object actual = client.eval(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }
//
//    @Test
//    public void testEvalsha() throws IOException {
//        Object actual = client.evalsha(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }
//
//    @Test
//    public void testExec() throws IOException {
//        Object actual = client.exec(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testExists() throws IOException {
        client.del(key);
        Object actual = client.exists("key1");
        Object expect = false;
        assertEquals(expect, actual);
    }

    @Test
    public void testExpire() throws IOException {
        client.del(key);
        Object actual = client.expire(key, 1);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testExpireat() throws IOException {
        long unixTime = (long)System.currentTimeMillis()/1000;
        Object actual = client.expireAt(key, unixTime);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testFlushall() throws IOException {
        Object actual = client.flushAll();
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testFlushdb() throws IOException {
        Object actual = client.flushDB();
        Object expect = OK;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testGeoadd() throws IOException {
//        Object actual = client.geoadd(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }
//
//    @Test
//    public void testGeohash() throws IOException {
//        Object actual = client.geohash(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }
//
//    @Test
//    public void testGeopos() throws IOException {
//        Object actual = client.geopos(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }
//
//    @Test
//    public void testGeodist() throws IOException {
//        Object actual = client.geodist(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }
//
//    @Test
//    public void testGeoradius() throws IOException {
//        Object actual = client.georadius(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }
//
//    @Test
//    public void testGeoradiusbymember() throws IOException {
//        Object actual = client.georadiusbymember(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testGet() throws IOException {
        client.del(key);
        client.set(key, value);
        Object actual = client.get(key);
        Object expect = value;
        assertEquals(expect, actual);
    }

    @Test
    public void testGetbit() throws IOException {
        client.del(key);
        Object actual = client.getbit(key, 2);
        Object expect = false;
        assertEquals(expect, actual);
    }

    @Test
    public void testGetrange() throws IOException {
        client.del(key);
        Object actual = client.getrange(key, 0, 8);
        Object expect = "";
        assertEquals(expect, actual);
    }

    @Test
    public void testGetset() throws IOException {
        client.set(key, "old");
        Object actual = client.getSet(key, "new");
        Object expect = "old";
        assertEquals(expect, actual);
    }

    @Test
    public void testHdel() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
        Object actual = client.hdel(key, "value");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testHexists() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
        Object actual = client.hexists(key, "value");
        Object expect = false;
        assertEquals(expect, actual);
    }

    @Test
    public void testHget() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
        Object actual = client.hget(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testHgetall() throws IOException {
        Map<String, String> actual = client.hgetAll(key);
        Object expect = "{a=b}";
        assertEquals(expect, actual.toString());
    }

    @Test
    public void testHincrby() throws IOException {
        Object actual = client.hincrBy(key, "field", 1);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testHincrbyfloat() throws IOException {
        Object actual = client.hincrByFloat(key, "field", 0.5);
        Object expect = 0.5;
        assertEquals(expect, actual);
    }

    @Test
    public void testHkeys() throws IOException {
        Set<String> actual = client.hkeys(key);
        assertEquals(0, actual.size());
    }

    @Test
    public void testHlen() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
        Object actual = client.hlen(key);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testHmget() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
        List<String> actual = client.hmget(key, "value");
        assertEquals(1, actual.size());
    }

    @Test
    public void testHmset() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
            Object actual = client.hmset(key, hash);
            Object expect = OK;
            assertEquals(expect, actual);
    }

    @Test
    public void testHset() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
        Object actual = client.hset(key, hash);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testHsetnx() throws IOException {
        Map<String, String> hash = new HashMap<String, String>();
        hash.put("a", "b");
        client.del(key);
        client.hset(key, hash);
        Object actual = client.hsetnx(key, "field", "value");
        Object expect = 1L;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testHstrlen() throws IOException {
//        Object actual = client.hstrlen(key, "field");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testHvals() throws IOException {
        List<String> actual = client.hvals(key);
        assertEquals(1, actual.size());
    }

    @Test
    public void testIncr() throws IOException {
        client.del(key);
        client.set(key, "0");
        Object actual = client.incr(key);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testIncrby() throws IOException {
        client.del(key);
        Object actual = client.incrBy(key, 1);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testIncrbyfloat() throws IOException {
        client.set(key, "1");
        Object actual = client.incrByFloat(key, .5);
        Object expect = 1.5;
        assertEquals(expect, actual);
    }

    @Test
    public void testInfo() throws IOException {
        String actual = client.info("section");
        Object expect = "";
        assertEquals(expect, actual);
    }

//    @Test
//    public void testLolwut() throws IOException {
//        Object actual = client.lolwut(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testKeys() throws IOException {
        Set<String> actual = client.keys("*");
        assertEquals(1, actual.size());
    }

    @Test
    public void testLastsave() throws IOException {
        Object actual = client.lastsave();
        assertNotNull(actual);
    }

    @Test
    public void testLindex() throws IOException {
        Object actual = client.lindex(key, 0);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testLinsert() throws IOException {
        ListPosition where = ListPosition.BEFORE;
        String pivot = "pivot";
        String value = "value";
        Object actual = client.linsert(key, where, pivot, value);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testLlen() throws IOException {
        client.del(key);
        client.lpush(key, "bla");
        Object actual = client.llen(key);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testLpop() throws IOException {
        Object actual = client.lpop(key);
        Object expect = "bla";
        assertEquals(expect, actual);
    }

    @Test
    public void testLpush() throws IOException {
        client.del(key);
        client.lpush(key, value);
        Object actual = client.lpush(key, "value");
        Object expect = 2L;
        assertEquals(expect, actual);
    }

    @Test
    public void testLpushx() throws IOException {
        client.del(key);
        client.lpush(key, value);
        Object actual = client.lpushx(key, "value");
        Object expect = 2L;
        assertEquals(expect, actual);
    }

    @Test
    public void testLrange() throws IOException {
        client.del(key);
        client.lpush(key, value);
        List<String> actual = client.lrange(key, 0, 1);
        Object expect = value;
        assertEquals(expect, actual.get(0));
    }

    @Test
    public void testLrem() throws IOException {
        Object actual = client.lrem(key, 0, "value");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testLset() throws IOException {
        client.del(key);
        client.lpush(key, "value");
        Object actual = client.lset(key, 0, "value");
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testLtrim() throws IOException {
        Object actual = client.ltrim(key, 1, 2);
        Object expect = OK;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testMemory() throws IOException {
//        Object actual = client.memoryDoctor(); // (key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testMget() throws IOException {
        client.del(key);
        List<String> actual = client.mget(key, "key2");
        assertEquals(2, actual.size());
    }

    @Test
    public void testMigrate() throws IOException {
        try {
            Object actual = client.migrate(host, port, key, destinationDb, timeout);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    int timeout = 0;
    int port = 0;;

    private String host = "host";

//    @Test
//    public void testModule() throws IOException {
//        client.moduleLoad("bla");
//        client.moduleUnload("bla");
//        Object actual = client.moduleList();
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testMonitor() throws IOException {
//        JedisMonitor monitor = new JedisMonitor() {
//            @Override
//            public void onCommand(String command) {
//            }
//        };
//        client.monitor(monitor); //key, "value");
    }

    @Test
    public void testMove() throws IOException {
        client.del(key);
        client.set(key, value);
        Object actual = client.move(key, 1);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testMset() throws IOException {
        Object actual = client.mset(key, "value");
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testMsetnx() throws IOException {
        Object actual = client.msetnx(key, "value");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testMulti() throws IOException {
//        Transaction transaction = client.multi(); //key, "value");
//    }

    @Test
    public void testObjectEncoding() throws IOException {
        Object actual = client.objectEncoding(key);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testObjectIdletime() throws IOException {
        Object actual = client.objectIdletime(key);
        Object expect = null;
        //assertNotNull(actual);
    }

    /*
    @Test
    public void testPersist() throws IOException {
        Object actual = client.persist(key);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testPexpire() throws IOException {
        Object actual = client.pexpire(key, 1000);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testPexpireat() throws IOException {
        Object actual = client.pexpireAt(key, 0);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testPfadd() throws IOException {
        client.del(key);
        client.lpush(key, value);
        Object actual = client.pfadd(key, "value");
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testPfcount() throws IOException {
        Object actual = client.pfcount(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testPfmerge() throws IOException {
        Object actual = client.pfmerge(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }
*/

    @Test
    public void testPing() throws IOException {
        Object actual = client.ping();
        Object expect = "PONG";
        assertEquals(expect, actual);
    }

    String value = "value";

    @Test
    public void testPsetex() throws IOException {
        long milliseconds = 200;
        Object actual = client.psetex(key, milliseconds, value);
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testPsubscribe() throws IOException {
//        JedisPubSub jedisPubSub = null;
//        client.psubscribe(jedisPubSub, "*", "*");
    }

    @Test
    public void testPubsub() throws IOException {
        String pattern = "*";
        List<String> actual = client.pubsubChannels(pattern);
        assertEquals(0, actual.size());
    }

    @Test
    public void testPttl() throws IOException {
        Object actual = client.pttl(key);
        Object expect = -1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testPublish() throws IOException {
        client.del(key);
        Object actual = client.publish(key, "value");
        Object expect = 1L;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testPunsubscribe() throws IOException {
//        Object actual = client.punsubscribe(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testQuit() throws IOException {
//        Object actual = client.quit();
//        Object expect = "OK";
//        assertEquals(expect, actual);
    }

    @Test
    public void testRandomkey() throws IOException {
        Object actual = client.randomKey();
        Object expect = dstkey;
        assertEquals(expect, actual);
    }

    @Test
    public void testReadonly() throws IOException {
//        Object actual = client.readonly();
//        Object expect = null;
//        assertEquals(expect, actual);
    }

//    @Test
//    public void testReadwrite() throws IOException {
//        Object actual = client.readwrite(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testRename() throws IOException {
        Object actual = client.rename(key, "value");
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testRenamenx() throws IOException {
        client.del(key);
        client.set(key,  value);
        Object actual = client.renamenx(key, "value");
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testRestore() throws IOException {
        try {
            client.del(key);
            Object actual = client.restoreReplace(key, ttl, serializedValue);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

//    @Test
//    public void testRole() throws IOException {
//        Object actual = client.role();
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testRpop() throws IOException {
        client.del(key);
        client.lpush(key, value);
        Object actual = client.rpop(key);
        Object expect = value;
        assertEquals(expect, actual);
    }

    @Test
    public void testRpoplpush() throws IOException {
        client.del(key);
        client.lpush(key, value);
        Object actual = client.rpoplpush(key, "value");
        Object expect = value;
        assertEquals(expect, actual);
    }

    @Test
    public void testRpush() throws IOException {
        client.del(key);
        client.lpush(key, value);
        Object actual = client.rpush(key, "value");
        Object expect = 2L;
        assertEquals(expect, actual);
    }

    @Test
    public void testRpushx() throws IOException {
        Object actual = client.rpushx(key, "value");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testSadd() throws IOException {
        Object actual = client.sadd(key, "value");
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testSave() throws IOException {
        Object actual = client.save();
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testScard() throws IOException {
        try {
            Object actual = client.scard(key);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testScript() throws IOException {
        Object actual = client.scriptLoad("x=1;");
        assertNotNull(actual);
    }

    @Test
    public void testSdiff() throws IOException {
        try {
            Object actual = client.sdiff(key, "value");
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testSdiffstore() throws IOException {
        Object actual = client.sdiffstore(key, "value");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testSelect() throws IOException {
        Object actual = client.select(0);
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testSet() throws IOException {
        Object actual = client.set(key, "value");
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testSetbit() throws IOException {
        client.del(key);
        Object actual = client.setbit(key, 0, "1");
        Object expect = false;
        assertEquals(expect, actual);
    }

    @Test
    public void testSetex() throws IOException {
        Object actual = client.setex(key, (int)(System.currentTimeMillis()/1000), value);
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testSetnx() throws IOException {
        Object actual = client.setnx(key, "value");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testSetrange() throws IOException {
        Object actual = client.setrange(key, 0, value);
        Object expect = (long)value.length();
        assertEquals(expect, actual);
    }

    @Test
    public void testShutdown() throws IOException {
//        Object actual = client.shutdown();
//        Object expect = null;
//        assertEquals(expect, actual);
    }

    @Test
    public void testSinter() throws IOException {
        client.sadd("key1", "a");
        client.sadd("key1", "b");
        client.sadd("key2", "c");
        client.sadd("key2", "d");
        client.sadd("key2", "e");

        Set<String> actual = client.sinter("key1", "key2");
        assertEquals(1, actual.size());
        assertTrue(actual.contains("c"));
    }

    @Test
    public void testSinterstore() throws IOException {
        client.sadd("key1", "a");
        client.sadd("key1", "b");
        client.sadd("key2", "c");
        client.sadd("key2", "d");
        client.sadd("key2", "e");

        Object actual = client.sinterstore("key", "key1", "key2");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testSismember() throws IOException {
        Boolean actual = client.sismember(key, member);
        Object expect = false;
        assertEquals(expect, actual);
    }

    @Test
    public void testSlaveof() throws IOException {
//        Object actual = client.slaveof(host, port);
//        Object expect = "QUEUED";
//        assertEquals(expect, actual);
    }

//    @Test
//    public void testReplicaof() throws IOException {
//        Object actual = client.replicaof(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testSlowlog() throws IOException {
        List<Slowlog> actual = client.slowlogGet();
        assertEquals(0, actual.size());
    }

    @Test
    public void testSmembers() throws IOException {
        Set<String> actual = client.smembers(key);
        assertEquals(0, actual.size());
    }

    @Test
    public void testSmove() throws IOException {
        Object actual = client.smove(srckey, dstkey, member);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testSort() throws IOException {
        try {
            Object actual = client.sort(key);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testSpop() throws IOException {
        Object actual = client.spop(key);
        Object expect = value;
        assertEquals(expect, actual);
    }

    @Test
    public void testSrandmember() throws IOException {
        Object actual = client.srandmember(key);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testSrem() throws IOException {
        Object actual = client.srem(key, "value");
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testStrlen() throws IOException {
        client.del(key);
        client.set(key,  value);
        Object actual = client.strlen(key);
        Object expect = (long)value.length();
        assertEquals(expect, actual);
    }

    @Test
    public void testSubscribe() throws IOException {
//        JedisPubSub jedisPubSub = new JedisPubSub() {
//        };
//        client.psubscribe(jedisPubSub, patterns);
    }

    @Test
    public void testSunion() throws IOException {

        client.del("key1");
        client.del("key2");
        client.sadd("key1", "a");
        client.sadd("key1", "b");
        client.sadd("key2", "c");

        Set<String> actual = client.sunion("key1", "key2");
        assertEquals(3, actual.size());
    }

    @Test
    public void testSunionstore() throws IOException {

        client.sadd("key1", "a");
        client.sadd("key1", "b");
        client.sadd("key1", "c");
        client.sadd("key2", "c");
        client.sadd("key2", "d");
        client.sadd("key2", "e");
        Long actual = client.sunionstore("key", "key1", "key2");
        Object expect = 5L;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testSwapdb() throws IOException {
//        Object actual = client.swapDB(0,1);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testSync() throws IOException {
        client.sync();
    }

//    @Test
//    public void testPsync() throws IOException {
//        Object actual = client.psync(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testTime() throws IOException {
//        Object actual = client.time();
//        Object expect = null;
//        assertEquals(expect, actual);
    }

//    @Test
//    public void testTouch() throws IOException {
//        Object actual = client.touch(key);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testTtl() throws IOException {
        client.del(key);
        client.set(key, value);
        Object actual = client.ttl(key);
        Object expect = -1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testType() throws IOException {
//        try {
//            client.del(key);
//            client.set(key,  value);
//            Object actual = client.type(key);
//            Object expect = "string";
//            assertEquals(expect, actual);
//        }
//        catch (JedisConnectionException e) {
//        }
//        catch (Exception e) {
//            fail(e.toString());
//        }
    }

//    @Test
//    public void testUnsubscribe() throws IOException {
//        Object actual = client.unsubscribe(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testUnlink() throws IOException {
//        Object actual = client.unlink(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testUnwatch() throws IOException {
        Object actual = client.unwatch();
        Object expect = OK;
        assertEquals(expect, actual);
    }

//    @Test
//    public void testWait() throws IOException {
//        Object actual = client.wait(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testWatch() throws IOException {
        Object actual = client.watch(key, "value");
        Object expect = OK;
        assertEquals(expect, actual);
    }

    @Test
    public void testZadd() throws IOException {
//        Object actual = client.zadd(key, score, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
    }

    @Test
    public void testZcard() throws IOException {
        client.del(key);
        client.zadd(dstkey, score, member);
        Object actual = client.zcard(key);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testZcount() throws IOException {
        client.del(key);
        client.zadd(dstkey, score, member);
        Object actual = client.zcount(key, 0, 1);
        Object expect = 0L;
        assertEquals(expect, actual);
    }

    @Test
    public void testZincrby() throws IOException {
        client.del(key);
        client.zadd(dstkey, score, member);
        Object actual = client.zincrby(key, .5, "member");
        Object expect = .5;
        assertEquals(expect, actual);
    }

    @Test
    public void testZinterstore() throws IOException {
        client.del(key);
        client.zadd(dstkey, score, member);
        Object actual = client.zinterstore(key, "value");
        Object expect = 5L;
        assertEquals(expect, actual);
    }

    @Test
    public void testZlexcount() throws IOException {
//        client.del(key);
//        client.zadd(dstkey, score, member);
//        Object actual = client.zlexcount(key, "0", "1");
//        Object expect = null;
//        assertEquals(expect, actual);
    }

    /*
    @Test
    public void testZpopmax() throws IOException {
        Object actual = client.zpopmax(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZpopmin() throws IOException {
        Object actual = client.zpopmin(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZrange() throws IOException {
        Object actual = client.zrange(key, 0, 1);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZrangebylex() throws IOException {
        Object actual = client.zrangeByLex(key, min, max);
        Object expect = null;
        assertEquals(expect, actual);
    }


    @Test
    public void testZrevrangebylex() throws IOException {
        Object actual = client.zrevrangeByLex(dstkey, max, min);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZrangebyscore() throws IOException {
        double min = 0, max=0;
        Set<String> actual = client.zrangeByScore(dstkey, min, max);
        assertEquals(0, actual.size());
    }

    @Test
    public void testZrank() throws IOException {
        Object actual = client.zrank(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZrem() throws IOException {
//        Object actual = client.zrem(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
    }

    @Test
    public void testZremrangebylex() throws IOException {
        try {
            Object actual = client.zremrangeByLex(dstkey, min, max);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testZremrangebyrank() throws IOException {
        client.del(dstkey);
        client.zadd(dstkey, score, member);
        Object actual = client.zremrangeByRank(dstkey, start, stop);
        Object expect = 1L;
        assertEquals(expect, actual);
    }

    @Test
    public void testZremrangebyscore() throws IOException {
        Object actual = client.zremrangeByScore(dstkey, min, max);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZrevrange() throws IOException {
        Object actual = client.zrevrange(dstkey, start, stop);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZrevrangebyscore() throws IOException {
        Object actual = client.zrevrangeByScore(dstkey, max, min);
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZrevrank() throws IOException {
        Object actual = client.zrevrank(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZscore() throws IOException {
        Object actual = client.zscore(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }

    @Test
    public void testZunionstore() throws IOException {
        Object actual = client.zunionstore(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
    }
    */

    @Test
    public void testScan() throws IOException {
        try {
            Object actual = client.scan(cursor);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testSscan() throws IOException {
        try {
            Object actual = client.sscan(key, "value");
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testHscan() throws IOException {
        try {
            Object actual = client.hscan(key, "value");
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testZscan() throws IOException {
        try {
        Object actual = client.zscan(key, "value");
        Object expect = null;
        assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

//    @Test
//    public void testXinfo() throws IOException {
//        Object actual = client.xinfo(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testXadd() throws IOException {
//        Map<String, String> hash = new HashMap<String, String>();
//        StreamEntryID id = null;
//        Object actual = client.xadd(dstkey, id, hash);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testXtrim() throws IOException {
//        Object actual = client.xtrim(dstkey, maxLen, approximateLength);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testXdel() throws IOException {
//        StreamEntryID ids = null;
//        Object actual = client.xdel(dstkey, ids);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testXrange() throws IOException {
//        StreamEntryID startId = null;
//        StreamEntryID endId = null;
//        Object actual = client.xrange(dstkey, startId, endId, count);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testXrevrange() throws IOException {
        StreamEntryID startId = null;
        StreamEntryID endId = null;
        try {
            Object actual = client.xrevrange(dstkey, startId, endId, count);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {
            String msg = e.getMessage();
        }
    }

//    @Test
//    public void testXlen() throws IOException {
//        Object actual = client.xlen(key);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testXread() throws IOException {
//        long block = 0;
//        Entry<String, StreamEntryID> streams = Entr
//        Object actual = client.xread(count, block, streams);
//        Object expect = null;
//        assertEquals(expect, actual);
    }

//    @Test
//    public void testXgroup() throws IOException {
//        Object actual = client.xgroupSetID(key, groupname, endId);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testXreadgroup() throws IOException {
//        String groupname = null;
//        String consumer = null;
//        long block = 0;
//        boolean noAck = false;
//        Entry<String, StreamEntryID> streams = null;
//        Object actual = client.xreadGroup(groupname, consumer, count, block, noAck, streams);
//        Object expect = null;
//        assertEquals(expect, actual);
    }

//    @Test
//    public void testXack() throws IOException {
//        Object actual = client.xack(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

//    @Test
//    public void testXclaim() throws IOException {
//        Object actual = client.xclaim(key, group, consumername, minIdleTime, newIdleTime, retries, force, startId);
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    @Test
    public void testXpending() throws IOException {
        try {
            Object actual = client.xpending(key, groupname, startId, endId, count, consumername);
            Object expect = null;
            assertEquals(expect, actual);
        }
        catch (JedisDataException e) {

        }
        catch (Exception e) {
            fail(e.toString());
        }
    }

//    @Test
//    public void testLatency() throws IOException {
//        Object actual = client.latency(key, "value");
//        Object expect = null;
//        assertEquals(expect, actual);
//    }

    protected static RedisServer server;
    protected static Jedis client;
    private String consumername = "consumername";

    byte[] serializedValue = "serializedValue".getBytes();
    int ttl;
    String group = "group";
    private String srckey = "srckey";
    private String dstkey = "dstkey";
    private String member = "member";
    private String patterns = "*";
    private double score = 1.0;
    private String min = "1";
    private String max = "2";
    private long start = 0;
    private long stop = 1;
    private long maxLen = 1;
    private boolean approximateLength = true;
    private StreamEntryID end;
    private int count;
    private long minIdleTime = 0;
    private long newIdleTime = 1;
    private int retries = 1;
    private boolean force;
    private String groupname = "groupname";
    private StreamEntryID startId = StreamEntryID.LAST_ENTRY;
    private StreamEntryID endId = StreamEntryID.LAST_ENTRY;
    private String cursor = "cursor";

    private String key = "key";
    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    private int destinationDb;

}