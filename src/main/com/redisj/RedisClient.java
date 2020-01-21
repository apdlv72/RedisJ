package com.redisj;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;

import com.redisj.RedisServer.Args;
import com.redisj.RedisServer.RESPReader;
import com.redisj.RedisServer.RESPWriter;

public class RedisClient {

    public static void main(String[] args) throws UnknownHostException, IOException {
        RedisClient client = new RedisClient("127.0.0.1", 6381);

        String rc1 = client.set("RedisClient.string", "test1");
        System.out.println(rc1);

        Long rc2 = client.lpush("RedisClient.list", "test2");
        System.out.println(rc2);
        Long rc3 = client.lpush("RedisClient.list", "test3");
        System.out.println(rc3);

        Long rc4 = client.llen("RedisClient.list");
        System.out.println(rc4);

        Collection<String> keys = client.keys("*");
        System.out.println(keys);
    }

    private Long llen(String key) throws IOException {
        writer.sendArray("LLEN", key);
        return reader.readNumber();
    }

    private Long lpush(String key, String ... values) throws IOException {
        ArrayList<String> list = new ArrayList<String>();
        list.add("LPUSH");
        list.add(key);
        for (String value : values) list.add(value);
        writer.sendArray(list);
        return reader.readNumber();
    }

    private String set(String key, String value) throws IOException {
        writer.sendArray("SET", key, value);
        return (String) reader.readStringOrList();
    }

    public RedisClient(String host, int port) throws UnknownHostException, IOException {
        socket = new Socket(host, port);
        writer = new RESPWriter(socket);
        reader = new RESPReader(socket);
    }

    public Collection<String> keys(String pattern) throws IOException {
        writer.sendArray("KEYS", pattern);
        Args list = reader.readList();
        return list;
    }

    protected Socket socket;
    protected RESPWriter writer;
    protected RESPReader reader;

}
