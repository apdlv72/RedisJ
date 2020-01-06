package com.redisj;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

import redis.clients.jedis.Jedis;

public class IntTestRedisServer {

    public static void main(String[] args) throws IOException {

        final int PORT = 6579;

        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));

        RedisServer server = new RedisServer(PORT);
        server.serveForEver(true);
        System.out.printf("RedisServer started on port %s\n", PORT);

        RedisServer.sleepMillis(1000);
        Jedis jedis = new Jedis("127.0.0.1", PORT, 300*1000);

        boolean a = true;
        boolean b = !true;
        boolean c = !true;
        boolean d = !true;

        if (a) {
            Set<String> keys = jedis.keys("*");
            System.out.println(keys);

            Set<String> keys2 = jedis.keys("*");
            System.out.println(keys2);
        }

        if (b) {
            String rc = jedis.mset("c", "3", "d", "4");
            System.out.println("mset: " + rc);
        }

        if (c) {
            Set<String> keys = jedis.keys("*");
            System.out.println(keys);

            Long rc = jedis.msetnx("c", "3", "d", "4");
            System.out.println("msetnx: " + rc);
        }

        if (d) {
            String rc1 = jedis.set("text", "one\r\ntwo\t2\r\nthree\r\n");
            System.out.println("set: " + rc1);

            String rc2 = jedis.get("text");
            System.out.println("get: " + rc2);
        }

        RedisServer.Database db0 = server.getDb(0);
        System.out.println("db0: " + db0);

        jedis.close();

        System.out.println("Press enter to stop server");
        @SuppressWarnings("unused")
        String line = console.readLine();
    }
}