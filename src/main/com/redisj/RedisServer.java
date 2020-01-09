package com.redisj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 *
 * @author apdlv72
 *
 * A very basic java implementation of a Redis server mainly for testing purposes,
 * e.g. unit tests. The server can be quickly created and started in a test setup
 * and stopped in test tear down and filled with values as needed without tampering
 * any real Redis server accidentally.
 *
 * Only a subset of Redis commands is supported @see {@link Worker.dispatchCommand}.
 *
 */
public class RedisServer {

    public static final int DEFAULT_THREAD_POOL_SIZE = 20;

    public static final int DEFAULT_PORT = 6379;

    public static final int DEFAULT_MAX_DB = 16;

    public RedisServer() {
        this(DEFAULT_PORT, DEFAULT_MAX_DB);
    }

    public RedisServer(int port) {
        this(port, DEFAULT_MAX_DB);
    }

    public RedisServer(int port, int maxDb) {
        this.port  = port;
        this.maxDb = maxDb;
        databases = new TreeMap<Integer, Database>();
    }

    public RedisServer withPersistence(File persDir) {
        return withPersistence(persDir.getAbsolutePath());
    }

    public RedisServer withPersistence() {
        this.persistDir = createTempDir();
        return this;
    }

    public RedisServer withPersistence(String dirName) {
        this.persistDir = dirName;
        return this;
    }

    public RedisServer withThreadPoolSize(int size) {
        this.threadPoolSize = size;
        return this;
    }

    public void serveForEver(boolean background) throws IOException {

        synchronized (this) {

            this.stopRequested = false;

            if (null!=portListener) {
                portListener.interrupt();
                portListener = null;
            }

            portListener  = new PortListener(port);
            startupThread = new StartupThread();
            startupThread.start();

            if (null!=this.persistDir) {
                try {
                    persistifier = new Persistifier(this, persistDir);
                    Map<Integer, Database> db = persistifier.load();
                    databases.putAll(db);
                    persistifier.start();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (background) portListener.start(); else portListener.run();
        }
    }

    public void stop() {
        try {
            if (null!=portListener) {
                portListener.stopRequested = true;
                portListener.socket.close();
                portListener.interrupt();
                portListener = null;
            }
            this.stopRequested = false;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void save() throws IOException {
        if (null!=persistifier) {
            persistifier.persist(databases);
        }
    }

    public void flush() {
        synchronized (databases) {

            Set<Integer> dbs = new TreeSet<Integer>(databases.keySet());
            for (int dbNumber : dbs) {
                Database db = databases.get(dbNumber);
                if (null!=db) {
                    synchronized (db) {
                        db.markDirty().clear();
                    }
                }
                onAfterCommand(dbNumber, 0, "FLUSH", null);
            }
        }
        onAfterCommand(-1, 0, "FLUSH", null);
    }

    public boolean isStarted() {
        return null!=portListener && null!=portListener.socket && portListener.socket.isBound();
    }

    public boolean waitUntilListening() {
        for (int retry=0;  retry<50; retry++) {
            try {
                Socket s = new Socket("127.0.0.1", port);
                s.close();
                return true;
            }
            catch (Exception e) {
                try { Thread.sleep(100); } catch (InterruptedException e1) {}
            }
        }
        return false;
    }

    public Database getDb(int num) {
        synchronized (databases) {
            Database db = databases.get(num);
            if (null==db) {
                databases.put(num, db=new Database(num));
            }
            return db;
        }
    }

    public void set(int db, String key, String value) {
        getDb(db).put(key, new Ageable(value));
    }

    public <T> T get(int db, String key) {
        Database d = getDb(db);
        Ageable  a = d.get(key);
        @SuppressWarnings("unchecked")
        T t = notExpired(a) ? (T)a.value : null;
        return t;
    }

    public void flushDb(int db) {
        getDb(db).clear();
    }

    public int getPort() {
        return port;
    }

    public void addCommandListener(RedisListener l) {
        synchronized (commandListeners) {
            commandListeners.add(l);
        }
    }

    public void removeCommandListener(RedisListener l) {
        synchronized (commandListeners) {
            commandListeners.remove(l);
        }
    }

    public void clearCommandListeners() {
        synchronized (commandListeners) {
            commandListeners.clear();
        }
    }

    protected ServerSocket createServerSocket(int port) throws IOException {
        return new ServerSocket(port);
    }

    protected ExecutorService createExecutor() {
        ExecutorService ex = Executors.newFixedThreadPool(this.threadPoolSize);
        return ex;
    }

    protected boolean checkMissingKeys(Set<String> missing) {
        return false;
    }

    protected void onServerStarting(int port) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onServerStarting(port);
                }
                catch (Exception e) {
                    logError(CN + ".onServerStarting: %s", e.getMessage());
                }
            }
        }
    }

    protected void onStartFailed(String cause) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onStartFailed(cause);
                }
                catch (Exception e) {
                    logError(CN + ".onStartFailed: %s", e.getMessage());
                }
            }
        }
    }

    protected void onServerStarted(String status) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onServerStarted(status);
                }
                catch (Exception e) {
                    logError(CN + ".onServerStarting: %s", e.getMessage());
                }
            }
        }
    }

    protected void onFileNotFound(int dbNum, File file) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onFileNotFound(dbNum, file);
                }
                catch (Exception e) {
                    logError(CN + "onFileNotFound. %s", e.getMessage());
                }
            }
        }
    }

    protected void onServerStopped(String reason) {

        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onServerStopping(reason, databases.size());
                }
                catch (Exception e) {
                    logError(CN + "onServerStopping. %s", e.getMessage());
                }
            }
        }

        try {
            if (null!=persistifier) {
                persistifier.interrupt();
                persistifier.persist(databases);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onServerStopped(reason);
                }
                catch (Exception e) {
                    logError(CN + "onServerStopped. %s", e.getMessage());
                }
            }
        }
    }

    protected void onSaving(int dbNumber, int keyCount, File dest) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onSaving(dbNumber, keyCount, dest);
                }
                catch (Exception e) {
                    logError(CN + ".onSaving: %s", e.getMessage());
                }
            }
        }
    }

    protected void onLoadingComplete(int dbCount) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onLoadingComplete(dbCount);
                }
                catch (Exception e) {
                    logError(CN + ".onLoadingComplete: %s", e.getMessage());
                }
            }
        }
    }

    protected void onDatabaseLoaded(int dbNumber, int keyCount, String reason) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onDatabaseLoaded(dbNumber, keyCount, reason);
                }
                catch (Exception e) {
                    logError(CN + ".onDatabaseLoaded: %s", e.getMessage());
                }
            }
        }
    }

    protected void onBeforeCommand(int dbNumber, int keyCount, String cmd, List<String> args) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onBeforeCommand(dbNumber, keyCount, cmd, args);
                }
                catch (Exception e) {
                    logError(CN + ".onBeforeCommand: %s", e.getMessage());
                }
            }
        }
    }

    protected void onAfterCommand(int dbNumber, int keyCount, String cmd, List<String> args) {
        synchronized (commandListeners) {
            for (RedisListener l : commandListeners) {
                try {
                    l.onAfterCommand(dbNumber, keyCount, cmd, args);
                }
                catch (Exception e) {
                    logError(CN + ".onAfterCommand: %s", e.getMessage());
                }
            }
        }
    }

    protected static void sleepMillis(int millis) {
        try { Thread.sleep(millis); } catch (Exception e) {}

    }

    protected static String createTempDir() {

        try {
            final File temp = File.createTempFile("redisj", "");

            if (!(temp.delete())) {
                throw new RuntimeException("Failed to delete file " + temp.getAbsolutePath());
            }

            if(!(temp.mkdir())) {
                throw new RuntimeException("Failed to create folder " + temp.getAbsolutePath());
            }

            return temp.getAbsolutePath();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected boolean notExpired(Ageable ageable) {
        return null!=ageable && (ageable.expires<0 || ageable.expires<now());
    }

    protected long now() {
        return System.currentTimeMillis();
    }

    protected void logInfo(String format, Object ... args) {
        String message = String.format(format, args);
        System.out.println(message);
    }

    protected void logError(String format, Object ... args) {
        String message = String.format(format, args);
        System.err.println(message);
    }

    protected String truncateString(String s) {
        if (s.length()>200) s = s.substring(0,200);
        s = s.replace("\r", "\\r");
        s = s.replace("\n", "\\n");
        return s;
    }

    protected WorkerMethod findWorkerMethod(String cmd) throws IOException {

        String name = cmd.toLowerCase();
        WorkerMethod found = methodCache.get(name);
        if (null!=found) {
            return found;
        }

        Method method = null;
        try {
            method = Worker.class.getDeclaredMethod(name, Args.class);
        }
        catch (Exception e) {
            return null;
        }

        RedisCommand anno = null==method ? null : method.getAnnotation(RedisCommand.class);
        if (null==anno) {
            return null;
        }

        found = new WorkerMethod(name, anno, method);
        methodCache.put(name, found);
        return found;
    }

    class StartupThread extends Thread {

        @Override
        public void run() {
            for (int i=0; i<10; i++) {
                try {
                    Thread.sleep(250);
                }
                catch (InterruptedException e) {
                }

                if (stopRequested) {
                    return;
                }
                else if (null!=portListener && portListener.isBound()) {
                    onServerStarted("STARTED");
                    return;
                }
            }
            onServerStarted("TIMEOUT");
        }
    }

    /**
     * This thread accepts new clients that connect on the listen socket and
     * spawns and starts new thread @see {@link Worker} for any new connections.
     */
    class PortListener extends Thread {

        public boolean stopRequested;
        protected volatile ServerSocket socket;

        boolean isBound() {
            return null!=socket && socket.isBound();
        }

        public PortListener(int port) throws BindException {

            super.setDaemon(true);

            this.port = port;
            try {
                socket   = createServerSocket(port);
                executor = createExecutor();
            }
            catch (BindException e) {
                throw e;
            }
            catch (IOException e) {
                String cause = "Port " + port + ": " + e.getMessage();
                onStartFailed(cause);
                throw new RuntimeException(cause, e);
            }
        }

        @Override
        public synchronized void start() {
            super.start();
            startTime = now();
        }

        @Override
        public void run() {
            serve();
        }

        protected void serve() {

            boolean first = true;
            while (!stopRequested && null!=socket) {

                if (first) {
                    first = false;
                    onServerStarting(port);
                }

                Socket clientSocket = null;
                try {
                    clientSocket = socket.accept();

                    totalConnectionsReceived++;

                    Worker command = new Worker(clientSocket);
                    executor.execute(command);
                    connectedClients++;
                }
                catch (Exception e) {

                    if (null!=startupThread) {
                        startupThread.interrupt();
                    }

                    SocketAddress addr = null;
                    try {
                        addr = clientSocket.getRemoteSocketAddress();
                    }
                    catch (Exception e2) {
                    }

                    String msg = e.getMessage();
                    if (!msg.contains("Socket closed")) {
                        logError(CN + ".serve: %s %s", addr, e.getMessage());
                    }
                }
            }

            executor.shutdownNow();

            String reason = stopRequested ? "Requested" : isInterrupted() ? "Interrupted" : "Socket closed";

            stopRequested = false;
            onServerStopped(reason);
        }

        protected int port;
        private ExecutorService executor;
    }

    class WorkerMethod {

        private int min;
        private int max;
        private int args;
        private boolean even;
        private boolean odd;

        public WorkerMethod(String name, RedisCommand anno, Method method) {
            this.name = name;
            //this.anno = anno;
            this.method = method;
            this.min  = anno.min();
            this.max  = anno.max();
            this.args = anno.args().length;
            this.even = anno.even();
            this.odd  = anno.odd();
        }

        String checkArguments(Args args) {

            WorkerMethod rm = this;
            int actual = args.size();
            if (rm.min>-1 || rm.max>-1) {
                if ((rm.min>-1 && actual<rm.min) || (rm.max>-1 && actual>rm.max)) {
                    return String.format("Wrong number of arguments for '%s' command. Expected %d - %d but actual number is %d",
                            rm.min, rm.max, actual);
                }
            }
            else {
                int expect = rm.args;
                if (actual!=expect) {
                    return String.format("Wrong number of arguments for '%s' command. Expected %d but actual number is %d",
                            name, expect, actual);
                }
            }
            if (rm.even && (actual%2)>0) {
                return String.format("Number of arguments for '%s' command must be even", name);
            }
            if (rm.odd && (actual%2)==0) {
                return String.format("Number of arguments for '%s' command must be odd", name);
            }
            return null;
        }

        public void invoke(Worker worker, Args args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            method.invoke(worker, args);
        }

        private String name;
        //private RedisCommand anno;
        private Method method;
    }

    /**
     * This thread handles communication on a client socket.
     * I continues to read commands from the client and send replies until
     * the client finally disconnects.
     */
    class Worker implements Runnable {

        public Worker(Socket clientSocket) {
            //super.setDaemon(true);
            this.socket = clientSocket;
        }

        @Override
        public void run() {

            boolean connected = socket.isConnected();
            boolean closed = socket.isClosed();

            do {
                try {
                    handleRequests();
                } catch (IOException e) {
                    logError(e.getMessage());
                }
                connected = null!=socket && socket.isConnected();
                closed    = null!=socket && socket.isClosed();
            }
            while (null!=socket && connected && !closed);

            try {
                if (null!=socket) {
                    socket.close();
                }
            } catch (IOException e) {
            }
            connectedClients--;
        }

        public void handleRequests() throws IOException {

            final String METHOD = CN + ".handleRequest: ";

            writer = new RESPWriter(socket);
            reader = new RESPReader(socket);

            int commands = 0;
            Args list = null;
            do {
                list = null;
                try {
                    list = reader.readList();
                    commands++;
                }
                catch (SocketException e) {
                    socket.close();
                    list = null;
                }
                catch (Exception e) {
                    String message = e.getMessage();
                    logError(METHOD + "readList: %s", message + " on " + socket.getRemoteSocketAddress() + " after " + commands + " commands");
                }

                if (null!=list) {
                    try {
                        String cmd  = (String) list.remove(0);
                        Args   args = list;

                        int num = selectedDb;
                        if (!commandListeners.isEmpty()) {
                            Database db = getSelectedDb();
                            int keys = db.size();
                            onBeforeCommand(num, keys, cmd, args);
                        }

                        dispatchCommand(cmd, args);

                        if (!commandListeners.isEmpty()) {
                            Database db = getSelectedDb();
                            int keys = db.size();
                            onAfterCommand(num, keys, cmd, args);
                        }
                    }
                    catch (RESPException e) {
                        String message = e.getMessage();
                        logError(METHOD + "dispatchCommand: %s", message);
                        writer.sendError("ERR", message);
                    }
                }
            }
            while (null!=list);

            socket.close();
            socket = null;
        }

        protected void dispatchCommand(String cmd, Args args) throws IOException {

            try {
                WorkerMethod rm = findWorkerMethod(cmd);
                if (null==rm) {
                    writer.sendError("WRONGCMD", "Unknown command '%s'", cmd);
                    return;
                }

                String error = rm.checkArguments(args);
                if (null!=error) {
                    writer.sendError("WRONGARGS", error);
                    return;
                }

                rm.invoke(this, args);
            }
            catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                if (cause instanceof ClassCastException) {
                    writer.sendError("WRONGTYPE", "Operation against a key holding the wrong kind of value");
                }
                else {
                    writer.sendError("EXCEPTION", "%s", e.getMessage());
                }
            }
            catch (ClassCastException e) {
                writer.sendError("WRONGTYPE", "Operation against a key holding the wrong kind of value");
            }
            catch (WrongNumberOfArgsException e) {
                writer.sendError("ERR", "wrong number of arguments for '%s' command", cmd);
            }
            catch (Exception e) {
                writer.sendError("EXCEPTION", "%s", e.getMessage());
            }
            finally {
                totalCommandsProcessed++;
            }
        }

        @RedisCommand(args = {"key", "field"})
        protected void hstrlen(Args args) throws IOException {

            String key   = args.get(0);
            String field = args.get(1);

            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                if (null!=a) {
                    Hash hash = (Hash) a.value;
                    String value = hash.get(field);
                    writer.sendNumber(value.length());
                    return;
                }
                writer.sendNumber(-1);
            }
        }

        @RedisCommand(args = {"key", "field"})
        protected void hget(Args args) throws IOException {

            String key   = args.get(0);
            String field = args.get(1);

            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                if (null!=a) {
                    Hash hash = (Hash) a.value;
                    String value = hash.get(field);
                    writer.sendString(value);
                    return;
                }
                writer.sendString(EMPTY_STRING);
            }
        }

        @RedisCommand(args = {"key"})
        protected void hgetall(Args args) throws IOException {

            String key = args.get(0);

            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                List<String> list = new ArrayList<String>();
                if (null!=a) {
                    Hash hash = (Hash) a.value;

                    for (Entry<String, String> entry : hash.entrySet()) {
                        list.add(entry.getKey());
                        list.add(entry.getValue());
                    }
                }
                writer.sendArray(list);
            }
        }

        @RedisCommand(args = {"key", "field", "amount"})
        protected void hincrby(Args args) throws IOException {

            String key   = args.get(0);
            String field = args.get(1);
            long incr = Long.parseLong(args.get(2));

            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                Hash hash = null;
                if (null==a) {
                    db.put(key, new Ageable(hash = new Hash()));
                }
                else {
                    hash = (Hash) a.value;
                }

                String value = hash.getOrDefault(field, "0");
                long sum = Long.parseLong(value)+incr;
                String string = ""+sum;
                hash.put(field, string);
                writer.sendNumber(sum);
            }
        }

        @RedisCommand(args = {"key", "field", "amount"})
        protected void hincrbyfloat(Args args) throws IOException {

            String key   = args.get(0);
            String field = args.get(1);
            double incr = Double.parseDouble(args.get(2));

            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                Hash hash = null;
                if (null==a) {
                    db.put(key, new Ageable(hash = new Hash()));
                }
                else {
                    hash = (Hash) a.value;
                }

                String value = hash.getOrDefault(field, "0");
                double sum = Double.parseDouble(value)+incr;
                String string = ""+sum;
                hash.put(field, string);
                writer.sendString(string);
            }
        }

        @RedisCommand(args = {"key", "field", "value"})
        protected void hsetnx(Args args) throws IOException {
            String key = args.get(0);
            String field = args.get(1);
            String value = args.get(2);
            _hset(key, field, value, true);
        }

        @RedisCommand(args = {"key", "field", "value"})
        protected void hset(Args args) throws IOException {
            String key = args.get(0);
            String field = args.get(1);
            String value = args.get(2);
            _hset(key, field, value, false);
        }

        @RedisCommand(args = {"key", "field"})
        protected void hexists(Args args) throws IOException {

            String key = args.get(0);
            String field = args.get(1);

            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                if (null!=a) {
                    Hash h = (Hash) a.value;
                    if (h.containsKey(field)) {
                        writer.sendNumber(1);
                        return;
                    }
                }
                writer.sendNumber(0);
            }
        }

        @RedisCommand(args = {"key"})
        protected void llen(Args args) throws IOException {

            String key = args.get(0);
            Database db = getSelectedDb();
            Ageable a = db.get(key, false);
            if (null==a) {
                writer.sendNumber(0);
            }
            else {
                List<Object> list = a.get();
                writer.sendNumber(list.size());
            }
        }

        @RedisCommand(args = {"key"})
        protected void lpop(Args args) throws IOException {
            String key = args.get(0);
            _pop(key, true);
        }

        @RedisCommand(args = {"key"})
        protected void rpop(Args args) throws IOException {
            String key = args.get(0);
            _pop(key, false);
        }

        @RedisCommand(args = {"key", "value"})
        protected void rpushx(String cmd) throws IOException {
            _todo(cmd);
        }

        @RedisCommand(args = {"key", "value"})
        protected void rpush(Args args) throws IOException {
            String key   = args.get(0);
            String value = args.get(1);
            _push(key, value, false);
        }

        @RedisCommand(args = {"key", "value"})
        protected void lpush(Args args) throws IOException {
            String key   = args.get(0);
            String value = args.get(1);
            _push(key, value, true);
        }

        @RedisCommand(args = {"key"})
        protected void type(Args args) throws IOException {

            String key = args.get(0);

            Ageable ageable = getSelectedDb().get(key, false);
            if (notExpired(ageable)) {
                Object value = ageable.value;
                if (value instanceof String) {
                    writer.write("+string\r\n".getBytes());
                }
                else if (value instanceof List) {
                    writer.write("+list\r\n".getBytes());
                }
                else if (value instanceof Set) {
                    writer.write("+set\r\n".getBytes());
                }
                else if (value instanceof Map) {
                    writer.write("+hash\r\n".getBytes());
                }
                else if (value instanceof ZSet) {
                    writer.write("+zset\r\n".getBytes());
                }
                else {
                    writer.write(NONE_BYTES);
                }
            }
            else {
                writer.write(NONE_BYTES);
            }
        }

        @RedisCommand(args = {"key", "amout"})
        protected void decrbyfloat(Args args) throws IOException {
            _incrDecr(args.get(0), false, args.get(1), true);
        }

        @RedisCommand(args = {"key", "amout"})
        protected void incrbyfloat(Args args) throws IOException {
            _incrDecr(args.get(0), true, args.get(1), true);
        }

        @RedisCommand(args = {"key", "amout"})
        protected void decrby(Args args) throws IOException {
            _incrDecr(args.get(0), false, args.get(1), false);
        }

        @RedisCommand(args = {"key", "amout"})
        protected void incrby(Args args) throws IOException {
            _incrDecr(args.get(0), true, args.get(1), false);
        }

        @RedisCommand(args = {"key"})
        protected void decr(Args args) throws IOException {
            _incrDecr(args.get(0), false, "1", false);
        }

        @RedisCommand(args = {"key"})
        protected void incr(Args args) throws IOException {
            _incrDecr(args.get(0), true, "1", false);
        }

        @RedisCommand(args = {"db"})
        protected void select(Args args) throws IOException {
            String num = args.get(0);
            selectedDb = Integer.parseInt(num);
            if (maxDb>-1 && selectedDb>=maxDb) {
                writer.sendError("ERR", "Invalid database");
            }
            else {
                writer.write("+OK\r\n".getBytes());
            }
        }

        @RedisCommand(args = {"pattern"})
        protected void keys(Args args) throws IOException {

            String glob = args.get(0);

            Pattern rex = createRegexFromGlob(glob);
            ArrayList<String> matches = new ArrayList<String>();

            StringBuilder sb = new StringBuilder();
            Database db = getSelectedDb();
            for (String key : db.keySet()) {
                if (rex.matcher(key).matches()) {
                    matches.add(key);
                }
            }

            sb.append("*").append(matches.size()).append(CRLF_STRING);
            for (String key : matches) {
                sb.append("$").append(key.length()).append(CRLF_STRING);
                sb.append(key).append(CRLF_STRING);
            }

            String string = sb.toString();
            writer.write(string.getBytes());
        }

        @RedisCommand(args= {"key"})
        protected void get(Args args) throws IOException {

            String key = args.get(0);

            Database db = getSelectedDb();
            Ageable ageable = db.get(key, false);
            if (null==ageable) {
                writer.write(EMPTY_BYTES);
            }
            else {
                String string = (String) ageable.value;
                writer.sendString(string);
            }
        }

        @RedisCommand(args = {"key", "value"})
        protected void set(Args args) throws IOException {
            _set(args, false);
        }

        @RedisCommand(args = {"key", "value"})
        protected void setnx(Args args) throws IOException {
            _set(args, true);
        }

        @RedisCommand(args = {"key", "value"})
        protected void append(Args args) throws IOException {

            String key = args.get(0);
            String value = args.get(1);

            Database db = getSelectedDb().markDirty();

            Ageable a = db.get(key, false);
            long len = 0;
            if (null==a) {
                db.put(key, new Ageable(value));
                len = value.length();
            }
            else {
                String s = a.value.toString() + value;
                a.value = s;
                len = s.length();
            }
            writer.sendNumber(len);
        }

        @RedisCommand(args = {"key1", "key2", "..."}, min=1)
        protected void mget(Args args) throws IOException {

            Database db = getSelectedDb();
            writer.sendArrayLength(args.size());

            for (String key : args) {
                Ageable ageable = db.get(key, false);
                if (null==ageable) {
                    writer.write(EMPTY_BYTES);
                }
                else {
                    writer.sendString(ageable.value.toString());
                }
            }
        }

        @RedisCommand(args = {"subcmd", "[option]"}, min=1, max=2)
        protected void client(Args args) throws IOException {

            String subcmd = args.get(0).toUpperCase();

            if ("ID".equals(subcmd)) {
                writer.sendString(clientId);
            }
            else if ("LIST".equals(subcmd)) {
                writer.sendString("unsupported");
            }
            else if ("KILL".equals(subcmd)) {
                writer.sendString("unsupported");
            }
            else if ("SETNAME".equals(subcmd)) {
                assertArgCount(args, 2);
                clientName = args.get(1);
                writer.write(OK_BYTES);
            }
            else if ("GETNAME".equals(subcmd)) {
                writer.sendString(clientName);
            }
            else {
                writer.sendError("ERR", "Invalid command");
            }
        }

        @RedisCommand(args = {"message"})
        protected void echo(Args args) throws IOException {

            String message = args.get(0);
            writer.sendString(message);
        }

        @RedisCommand(args = {"key", "field1", "field2", "field3", "..."}, min=2)
        protected void hmget(Args args) throws IOException {

            String key = args.get(0);
            Database db = getSelectedDb();

            synchronized (db) {
                Ageable a = db.get(key, false);
                List<String> list = new ArrayList<String>();
                if (null!=a) {
                    Hash hash = (Hash) a.value;
                    for (int i=1, len=args.size(); i<len; i++) {
                        String value = hash.get(args.get(i));
                        list.add(value);
                    }
                }
                writer.sendArray(list);
            }
        }

        @RedisCommand(args = {"key", "field1", "value1", "field2", "value2", "..."}, odd=true, min=3)
        protected void hmset(Args args) throws IOException {

            String   key = args.get(0);
            Database db  = getSelectedDb();

            synchronized (db) {
                Ageable a = db.get(key, false);
                Hash hash = null;
                if (null==a) {
                    db.put(key, new Ageable(hash = new Hash()));
                }
                else {
                    hash = (Hash) a.value;
                }

                for (int i=1, len=args.size(); i<len; i+=2) {
                    hash.put(args.get(i), args.get(i+1));
                }

                writer.sendString("OK");
            }
        }

        @RedisCommand(args = {"key"})
        protected void hkeys(Args args) throws IOException {

            String key  = args.get(0);
            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                List<String> list = new ArrayList<String>();
                if (null!=a) {
                    Hash hash = (Hash) a.value;
                    for (String field : hash.keySet()) {
                        list.add(field);
                    }
                }
                writer.sendArray(list);
            }
        }

        @RedisCommand(args = {"key"})
        protected void hvals(Args args) throws IOException {

            String key  = args.get(0);
            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                List<String> list = new ArrayList<String>();
                if (null!=a) {
                    Hash hash = (Hash) a.value;
                    for (String field : hash.values()) {
                        list.add(field);
                    }
                }
                writer.sendArray(list);
            }
        }

        @RedisCommand(args = {"key"})
        protected void hlen(Args args) throws IOException {

            String key  = args.get(0);
            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                if (null!=a) {
                    Hash hash = (Hash) a.value;
                    writer.sendNumber(hash.size());
                }
                else {
                    writer.sendNumber(0);
                }
            }
        }

        @RedisCommand(args = {})
        protected void save(Args args) throws IOException {

            Persistifier pers = new Persistifier(RedisServer.this, "/tmp/radisj");
            pers.persist(databases);

            writer.write(OK_BYTES);
        }

        @RedisCommand(args = {})
        protected void ping(Args args) throws IOException {
            writer.sendString("PONG");
        }

        @RedisCommand(args = {"key1", "key2", "...", "timeout"}, min=2)
        protected void blpop(Args args) throws IOException {
            _bpop(true, args);
        }

        @RedisCommand(args = {"key"})
        protected void brpop(Args args) throws IOException {
            _bpop(false, args);
        }

        @RedisCommand(args={"key"})
        protected void del(Args args) throws IOException {
            String key = args.get(0);
            Ageable ageable = getSelectedDb().markDirty().remove(key);
            writer.sendNumber(notExpired(ageable) ? 1 : 0);
        }

        @RedisCommand(args = {})
        protected void flushdb(Args args) throws IOException {
            getSelectedDb().markDirty().clear();
            writer.write(OK_BYTES);
        }

        @RedisCommand(args = {"key"})
        protected void ttl(Args args) throws IOException {

            String key = args.get(0);
            Database db = getSelectedDb();
            long ttl = -2;

            Ageable ageable = db.get(key, false);
            if (notExpired(ageable)) {
                if (ageable.expires<0) {
                    ttl = -1;
                }
                else {
                    ttl = (now()-ageable.expires)/1000;
                }
            }
            else {
                ttl = -2;
            }

            writer.sendNumber(ttl);
        }

        @RedisCommand(args = {"key", "ttl"})
        protected void expire(Args args) throws IOException {

            String key = args.get(0);
            String ttl = args.get(1);

            Database db = getSelectedDb().markDirty();
            int seconds = Integer.parseInt(ttl);

            Ageable ageable = db.get(key, false);
            if (notExpired(ageable)) {
                ageable.expires = now()+1000*seconds;
                //output.write(":0\r\n".getBytes());
                writer.sendNumber(0);
            }
            else {
                //output.write(":1\r\n".getBytes());
                writer.sendNumber(1);
            }
        }

        @RedisCommand(args = {"key1", "val1", "key2", "val2"}, min=2, even=true)
        protected void mset(Args args) throws IOException {
            _mset(args, false);
        }

        @RedisCommand(args = {"key1", "val1", "key2", "val2"}, min=2, even=true)
        protected void msetnx(Args args) throws IOException {
            _mset(args, true);
        }

        @RedisCommand(args = {})
        protected void info(Args args) throws IOException {

            StringBuilder sb = new StringBuilder();

            long uptime_in_seconds = (now()-startTime)/1000;

            sb.append("# Server\r\n");
            sb.append("redis_version:2.8.13 ***** THIS IS NOT REAL REDIS BUT A VERY BASIC JAVA PORT *****\r\n");
            sb.append(String.format("tcp_port:%d\r\n", port));
            sb.append(String.format("uptime_in_seconds:%d\r\n", uptime_in_seconds));
            sb.append(CRLF_STRING);

            sb.append("# Stats\r\n");
            sb.append(String.format("total_connections_received:%d\r\n", totalConnectionsReceived));
            sb.append(String.format("total_commands_processed:%d\r\n", totalCommandsProcessed));
            sb.append(CRLF_STRING);

            sb.append("# Clients\r\n");
            sb.append(String.format("connected_clients:%d\r\n", connectedClients));
            sb.append(String.format("client_longest_output_list:%d\r\n", clientLongestOutputList));
            sb.append(String.format("client_biggest_input_buf:%d\r\n", clientBiggestInputBuf));
            sb.append(String.format("blocked_clients:%d\r\n", blockedClients));
            sb.append(CRLF_STRING);

            sb.append("# Keyspace\r\n");
            Set<Integer> keys = null;
            synchronized (databases) {
                keys = databases.keySet();
            }

            for (Integer num : keys) {
                Database db = RedisServer.this.getDb(num);
                int count = db.size();
                long expires = 0;
                long avgTtl = 0;
                sb.append(String.format("db%d:keys=%d,expires=%d,avg_ttl=%d\r\n", num, count, expires, avgTtl ));
            }

            writer.sendReply(sb);
        }

        protected void _set(List<String> args, boolean nx) throws IOException {

            assertArgCount(args, 2);
            String key  = args.get(0);
            String value = args.get(1);

            logInfo("SET: " + key + " (" + value.length() + " chars)");

            Database db = getSelectedDb().markDirty();
            if (nx) {
                if (db.containsKey(key)) {
                    writer.sendError("ERR", "Key exists");
                    return;
                }
            }

            db.put(key, new Ageable(value));
            writer.write(OK_BYTES);
        }

        protected void _mset(List<String> args, boolean nx) throws IOException {

            Database db = getSelectedDb().markDirty();

            if (nx) {
                for (int i=1; i<args.size(); i+=2) {

                    String key = args.get(i);

                    Ageable a = db.get(key, false);
                    if (notExpired(a)) {
                        logInfo("Key exists: " + key);
                        writer.write(":0\r\n".getBytes());
                        return;
                    }
                }
            }

            int count = 0;
            for (int i=0; i<args.size(); i+=2) {

                String key = args.get(i);
                String val = args.get(i+1);

                if (nx) {
                    Ageable a = db.get(key, false);
                    if (notExpired(a)) {
                        logInfo("Key exists: " + key);
                    }
                }

                db.put(key, new Ageable(val));
                count ++;
            }

            if (nx) {
                writer.sendNumber(count);
            }
            else {
                writer.write(OK_BYTES);
            }
        }

        protected void _incrDecr(String key, boolean incr, String amount, boolean _float) throws IOException {

            Database db = getSelectedDb().markDirty();
            Ageable a = db.get(key, false);
            if (null==a) {
                db.put(key, a = new Ageable("0"));
            }

            if (_float) {
                Double b = (incr ? 1 : -1) * toDouble(amount);
                String s = a.get();
                Double l = toDouble(s)+b;
                s = l.toString();;
                a.value  = s;
                writer.sendString(s);
            }
            else {
                Long    b = (incr ? 1 : -1) * toLong(amount);
                String  s = a.get();
                Long    l = toLong(s)+b;
                a.value = l.toString();;
                writer.sendNumber(l);
            }
        }

        protected void _todo(String cmd) throws IOException {
            _todo(cmd);
        }

        protected void _bpop(boolean left, List<String> args) throws IOException {

            String last = args.get(args.size()-1);
            Long timeout = toLong(last);
            long expires = (timeout>0) ? now()+1000*timeout : -1;

            Database db = getSelectedDb().markDirty();
            Object item = null;
            boolean expired = false;
            for (;!portListener.stopRequested && !expired;) {

                for (int i=0, len=args.size()-1; i<len; i++) {

                    String key = args.get(i);
                    Ageable a = db.get(key, false);
                    if (null!=a) {
                        @SuppressWarnings("unchecked")
                        List<Object> list = (List<Object>) a.value;
                        if (list.size()>0) {
                            if (left) {
                                item = list.remove(0);
                            }
                            else {
                                item = list.remove(list.size()-1);
                            }
                        }
                    }

                    if (null!=item) {
                        writer.sendArray(key, item.toString());
                        return;
                    }

                    if (timeout>0 && now()>expires) {
                        expired = true;
                    }

                    if (portListener.stopRequested || expired) {
                        break;
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            writer.write(EMPTY_BYTES);
        }

        protected void _pop(String key, boolean left) throws IOException {
            Database db = getSelectedDb().markDirty();
            Object item = null;
            Ageable a = db.get(key, false);
            if (null!=a) {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) a.value;
                if (list.size()>0) {
                    if (left) {
                        item = list.remove(0);
                    }
                    else {
                        item = list.remove(list.size()-1);
                    }
                }
            }

            if (null==item) {
                writer.write(EMPTY_BYTES);
            }
            else {
                writer.sendString(item.toString());
            }
        }

        protected void _push(String key, String val, boolean left) throws IOException {
            Database db = getSelectedDb().markDirty();
            Ageable a = db.get(key, false);
            if (null==a) {
                db.put(key, a = new Ageable(new ArrayList<Object>()));
            }

            Object obj = a.value;
            if (!(obj instanceof List)) {
                writer.sendError("WRONGTYPE", "Operation against a key holding the wrong kind of value");
            }
            else {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) obj;
                if (left) {
                    list.add(0, val);
                }
                else {
                    list.add(val); /// at end
                }
                writer.sendNumber(list.size());
            }
        }

        protected void _hset(String key, String field, String value, boolean nx) throws IOException {
            Database db = getSelectedDb();
            synchronized (db) {
                Ageable a = db.get(key, false);
                Hash hash = null;
                if (null==a) {
                    db.put(key, new Ageable(hash = new Hash()));
                }
                else {
                    hash = (Hash) a.value;
                }

                if (nx && hash.contains(field)) {
                    writer.sendNumber(0);
                }
                else {
                    hash.put(field, value);
                    writer.sendNumber(1);
                }
            }

        }

        protected void assertArgCount(List<String> args, int expected) throws WrongNumberOfArgsException {
            if (null==args || args.size()!=expected) {
                throw new WrongNumberOfArgsException();
            }
        }

        protected Pattern createRegexFromGlob(String glob) {
            String out = "^";
            for(int i = 0; i < glob.length(); ++i)
            {
                final char c = glob.charAt(i);
                switch(c)
                {
                case '*': out += ".*"; break;
                case '?': out += '.'; break;
                case '.': out += "\\."; break;
                case '\\': out += "\\\\"; break;
                default: out += c;
                }
            }
            out += '$';
            return Pattern.compile(out);
        }

        protected Database getSelectedDb() {
            return RedisServer.this.getDb(selectedDb);
        }

        protected Long toLong(String s) {
            return null==s ? null : Long.parseLong(s);
        }

        protected Double toDouble(String s) {
            return null==s ? null : Double.parseDouble(s);
        }

        protected String clientId;
        protected String clientName;
        protected int selectedDb;
        protected RESPReader reader;
        protected RESPWriter writer;
        protected Socket socket;
    }

    /**
     * This class implements the reading part of the RESP protocol, @see https://redis.io/topics/protocol
     */
    public class RESPReader {

        public RESPReader(InputStream is) throws IOException {
            this.br = new BufferedReader(new InputStreamReader(is));
            this.socket = null;
        }

        public RESPReader(Socket socket) throws IOException {
            this(socket.getInputStream());
            this.socket = socket;
        }

        public Object readStringOrList() throws IOException {

            String line = br.readLine();
            int count = Integer.parseInt(line.substring(1));
            if (line.startsWith("$")) {
                char[] cbuf = new char[count];
                readComplete(cbuf);
                readCrLf();
                return new String(cbuf);
            }
            else if (line.startsWith("*")) {
                return readList(count);
            }
            throw new RESPException("Expected $ or * but got " + truncateString(line));
        }

        public String readString() throws IOException {

            String line = br.readLine();
            if (null==line) {
                return null; // EOF
            }

            if (!line.startsWith("$")) {
                throw new RESPException("Expected $ but got " + truncateString(line));
            }

            int length = Integer.parseInt(line.substring(1));
            String s = readString(length);
            return s;
        }

        public Args readList() throws IOException {

            String line = br.readLine();
            if (null==line) {
                SocketAddress addr = null==socket ? null :  socket.getRemoteSocketAddress();
                logError("RESPReader: connection closed %s", addr);
                return null; // EOF
            }

            if (!line.startsWith("*")) {
                throw new RESPException("Expected * but got " + truncateString(line));
            }

            int count = Integer.parseInt(line.substring(1));
            Args list = readList(count);
            return list;
        }

        public Long readNumber() throws IOException {
            String line = br.readLine();
            if (!line.startsWith(":")) {
                throw new RESPException("Expected : but got " + truncateString(line));
            }
            long number = Long.parseLong(line.substring(1));
            return number;
        }

        protected Args readList(int count) throws IOException {
            Args list = new Args();
            for (int i=0; i<count; i++) {
                String s = readString();
                list.add(s);
            }
            return list;
        }

        protected String readString(int length) throws IOException {
            char[] cbuf = new char[length];
            readComplete(cbuf);
            readCrLf();

            String s = new String(cbuf);
            return s;
        }

        protected void readComplete(char[] cbuf) throws IOException {

            int length = cbuf.length;
            int count  = 0;
            while (count<cbuf.length) {

                int read = br.read(cbuf, count, length-count);
                if (read>0) {
                    count+=read;
                }
                else if (read<0) {
                    break;
                }
            }
            if (count!=length) {
                throw new IOException("Incomplete read. Expected " + length + " bytes but actually got only " + count);
            }
        }

        protected void readCrLf() throws IOException {
            char cbuf[] = new char[2];
            readComplete(cbuf);
            if ('\r'!=cbuf[0]) throw new RESPException("Invalid line end");
            if ('\n'!=cbuf[1]) throw new RESPException("Invalid line end");
        }

        private BufferedReader br;
        private Socket socket;
    }

    /**
     * This class implements the writing part of the RESP protocol, @see https://redis.io/topics/protocol
     */
    public class RESPWriter {

        public RESPWriter(Socket socket) throws IOException {
            this(socket.getOutputStream());
            this.socket = socket;
        }

        public RESPWriter(OutputStream output) {
            this.output = output;
        }

        public void flush() throws IOException {
            output.flush();
        }

        public void write(byte[] data) throws IOException {
            output.write(data);
        }

        public void sendReply(StringBuilder sb) throws IOException {
            String string = sb.toString();
            byte[] data = string.getBytes(StandardCharsets.UTF_8);

            String lenStr = String.format("$%d\r\n", data.length);

            output.write(lenStr.getBytes());
            output.write(data);
            output.write(CRLF_BYTES);
            output.flush();
        }

        public void sendArray(String ... strings) throws IOException {
            sendArrayLength(strings.length);
            for (String s : strings) {
                sendString(s);
            }
        }

        public void sendArray(List<String> strings) throws IOException {
            sendArrayLength(strings.size());
            for (String s : strings) {
                if (null==s) {
                    write(EMPTY_BYTES);
                }
                else {
                    sendString(s);
                }
            }
        }

        public void sendArrayLength(int len) throws IOException {
            output.write(("*" + len).getBytes());
            output.write(CRLF_BYTES);
        }

        public void sendNumber(long l) throws IOException {
            output.write((":" + l).getBytes());
            output.write(CRLF_BYTES);
        }

        public void sendString(String s) throws IOException {
            if (null==s) {
                output.write(EMPTY_BYTES);
            }
            else {
                String formatted = String.format("$%d\r\n%s\r\n", s.length(), s);
                byte[] bytes = formatted.getBytes();
                output.write(bytes);
            }
        }

        public void sendError(String category, String format, Object ... args) throws IOException {

            String formatted = String.format(format, args);
            logError(CN + ".RESPWriter: " + formatted);
            String line = "-" + category + " " + formatted + "\r\n";
            output.write(line.getBytes(StandardCharsets.UTF_8));
            output.flush();
        }

        public void close() throws IOException {
            output.close();
        }

        @SuppressWarnings("unused")
        private Socket socket;
        private OutputStream output;
    }

    /**
     * Base class for any exceptions thrown in result of a RESP protocol issue.
     */
    @SuppressWarnings("serial")
    class RESPException extends RuntimeException {
        public RESPException(String msg) {
            super(msg);
        }
        public RESPException(String msg, Throwable cause) {
            super(msg, cause);
        }
    }

    /**
     * Exception thrown when a RESP command was received with a wrong number of arguments.
     */
    @SuppressWarnings("serial")
    class WrongNumberOfArgsException extends RESPException {
        public WrongNumberOfArgsException() {
            super("Wrong number of arguments");
        }
    }

    /**
     * This class implements reading and writing all databases to/from persistent storage (disk);
     */
    class Persistifier extends Thread {

        public Persistifier(RedisServer redisServer) {
            this(redisServer, createTempDir());
        }

        public Persistifier(RedisServer redisServer, String dirname) {
            this.redisServer = redisServer;
            this.dir = new File(dirname);
            this.dir.mkdirs();
        }

        @Override
        public void interrupt() {
            this.stopRequested = true;
            super.interrupt();
        }

        @Override
        public synchronized void start() {
            this.stopRequested = false;
            super.start();
        }

        @Override
        public void run() {

            final String info = getInfo();

            while (!this.stopRequested) {
                // 10 minutes:
                sleepMillis(10*60*1000);

                if (!stopRequested) {
                    try {
                        logInfo("%s: Persisting databases to disk", info);
                        persist(databases);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

            logInfo("%s: terminating", info);
        }

        public Map<Integer, Database> load() throws IOException {

            logInfo("%s: loading databases from %s", getInfo(), dir);

            Map<Integer, Database> map = new HashMap<Integer, Database>();
            for (int num=0; num<16; num++) {

                try {
                    Database db = load(num);
                    if (null!=db) {
                        map.put(num, db);
                        int keyCount = db.size();
                        onDatabaseLoaded(num, keyCount, "OK");
                    }
                }
                catch (Exception e) {
                    onDatabaseLoaded(num, -1, "EXCEPTION " + e.getMessage());
                }

            }

            onLoadingComplete(map.size());
            return map;
        }

        public void persist(Map<Integer, Database> databases) throws IOException {
            synchronized (databases) {

                final String info = getInfo();
                logInfo("%s: saving %d databases", info, databases.size());

                for (Integer num : databases.keySet()) {
                    Database db = databases.get(num);
                    if (db.dirty) {
                        this.persist(db);
                    }
                    else {
                        logInfo("%s: database %d was not modified", info, num);
                    }
                }
            }
        }


        public String getInfo() {
            return getClass().getSimpleName() + "[" + redisServer.getPort() + "]";
        }

        public void persist(Database db) throws IOException {

            String info = getInfo();

            int num  = db.getNumber();
            File tmp  = new File(dir, String.format("db%d.tmp", num));
            File dest = getFileForDb(num);

            FileOutputStream fos = new FileOutputStream(tmp);
            RESPWriter writer = new RESPWriter(fos);

            int keyCount = -1;
            synchronized (db) {
                Set<String> keys = db.keySet();
                keyCount = keys.size();

                logInfo("%s: saving %d keys in db %d to %s", info, keyCount, databases.size(), tmp);
                onSaving(num, keyCount, dest);

                for (String key : keys) {

                    Ageable a = db.get(key, false);
                    if (null==a) {
                        // expired right now??
                        continue;
                    }

                    Object obj = a.value;
                    if (obj instanceof String) {
                        String string = (String) obj;
                        writer.sendString(key);
                        writer.sendNumber(a.expires);
                        writer.sendString((String)string);
                    }
                    else if (obj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<String> list = (List<String>) obj;
                        writer.sendString(key);
                        writer.sendNumber(a.expires);
                        writer.sendArray(list);
                    }
                    else {
                        throw new RuntimeException("Unsupported type " + obj.getClass());
                    }
                }
            }
            writer.close();

            Database check = load(num, tmp, true);
            if (check.size() != keyCount) {

                Set<String> missing = new TreeSet<String>();
                for (String key : db.keySet()) {
                    if (!check.containsKey(key)) {
                        missing.add(key);
                    }
                }

                if (!checkMissingKeys(missing)) {
                    String msg = String.format("Failed to save database %d. Wrote %d keys, read %d keys. Missing: %s",
                            num, keyCount, check.size(), missing);
                    logError("%s", msg);
                    throw new RuntimeException(msg);
                }
            }

            dest.delete();
            logInfo("%s: renaming %s -> %s", getInfo(), tmp, dest);
            tmp.renameTo(dest);
        }

        protected Database load(int num) throws IOException {
            File file = getFileForDb(num);
            return load(num, file, false);
        }

        protected Database load(int num, File file, boolean testWise) throws IOException {

            if (!file.isFile()) {
                onFileNotFound(num, file);
                return null;
            }

            if (!testWise) {
                logInfo("%s: loading: %s", getInfo(), file);
            }

            FileInputStream fis = new FileInputStream(file);
            RESPReader reader = new RESPReader(fis);

            Database db = new Database(num);
            boolean done = false;
            do {
                String key = reader.readString();
                if (null==key) {
                    done = true;
                }
                else {
                    Long   expires = reader.readNumber();
                    Object value   = reader.readStringOrList();
                    Ageable a = new Ageable(value, expires);
                    db.put(key, a);
                }
            }
            while (!done);

            if (!testWise) {
                logInfo("%s: loaded:  %s: %d keys", getInfo(), file, db.size());
            }

            return db;
        }

        protected File getFileForDb(int num) {
            File dest = new File(dir, String.format("db%d%s", num, SUFFIX));
            return dest;
        }

        protected volatile boolean stopRequested;
        protected final String SUFFIX = ".redisj";
        protected File dir;
        protected RedisServer redisServer;
    }


    /**
     * This class represents a single Redis database.
     */
    @SuppressWarnings("serial")
    public class Database extends LinkedHashMap<String, Ageable> {

        public Database(int number) {
            this.number = number;
        }

        public Database markDirty() {
            this.dirty = true;
            return this;
        }

        public int getNumber() {
            return number;
        }

        public void set(String ... keyValues) {
            for (int i=0, len=keyValues.length; i<len; i+=2) {
                super.put(keyValues[i], new Ageable(keyValues[i+1]));
            }
        }

        public Ageable get(String key, boolean returnExpired) {
            Ageable a = super.get(key);
            if (notExpired(a) || returnExpired) {
                return a;
            }
            return null;
        }

        public <T> T get(String key) {
            Ageable a = get(key, false);
            Object o = null==a ? null : a.value;
            @SuppressWarnings("unchecked")
            T t = (T) o;
            return t;
        }

        protected int number;
        protected boolean dirty;
    }

    /**
     * This class represents a single key/value pair in a Redis database along
     * with an expiration value.
     */
    class Ageable {

        public Ageable(Object value) {
            this.value = value;
            this.expires = -1;
        }

        public Ageable(Object value, Long expires) {
            this.value = value;
            this.expires = expires;
        }

        public <T> T get() {
            @SuppressWarnings("unchecked")
            T t = (T) value;
            return t;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[value=" + value + ", expires=" + expires + "]";
        }

        long expires;
        Object value;
    }

    // TODO: Implement support for ZSet, Hash, Bits, Sets
    class ZSet {
    }

    @SuppressWarnings("serial")
    class Hash extends LinkedHashMap<String,String> {

        public Hash() {
            super();
        }

        public boolean contains(String field) {
            return super.containsKey(field);
        }
    }

    public interface RedisListener {

        /**
         * Called when server started.
         * @param port
         */
        public void onServerStarting(int port);

        public void onStartFailed(String cause);

        public void onLoadingComplete(int dbCount);

        public void onSaving(int dbNumber, int keyCount, File dest);

        public void onServerStarted(String status);

        public void onFileNotFound(int dbNum, File file);

        public void onServerStopping(String reason, int size);

        /**
         * Called when server was stopped.
         */
        public void onServerStopped(String reason);

        /**
         * Called when database was loaded from persistent storage
         * @param dbNum Database number
         * @param keyCount Number of key/value pairs loaded
         * @param result A string describing loading success or failure.
         */
        public void onDatabaseLoaded(int dbNum, int keyCount, String result);

        /**
         * Called before command execution.
         * @param dbNum Currently selected database
         * @param keyCount Number of key/value pairs before command execution.
         * @param cmd The command to be executed
         * @param cmd Command arguments
         */
        public void onBeforeCommand(int dbNum, int keyCount, String cmd, List<String> args);

        /**
         * Called after command execution.
         * @param dbNum Currently selected database
         * @param keyCount Number of key/value pairs after command execution.
         * @param cmd The command that was executed
         * @param cmd Command arguments
         */
        public void onAfterCommand(int dbNum, int keyCount, String cmd, List<String> args);

    }

    @SuppressWarnings("serial")
    class Args extends ArrayList<String> {

    }

    /**
     * We need this annotation at runtime to find methods matching redis command:
     */
    @Retention( RetentionPolicy.RUNTIME )
    public @interface RedisCommand {
        String[] args();

        boolean even() default false;
        boolean odd() default false;

        int min() default -1;
        int max() default -1;
    }

    static final String CN = RedisServer.class.getSimpleName();

    protected static final String CRLF_STRING  = "\r\n";
    protected static final byte[] CRLF_BYTES   = CRLF_STRING.getBytes();

    protected static final String EMPTY_STRING = "$-1\r\n";
    protected static final byte[] EMPTY_BYTES  = EMPTY_STRING.getBytes();

    protected static final byte[] OK_BYTES   = "+OK\r\n".getBytes();
    protected static final byte[] NONE_BYTES = "+none\r\n".getBytes();

    protected Map<String, WorkerMethod> methodCache = new HashMap<String, RedisServer.WorkerMethod>();

    protected List<RedisListener> commandListeners = new ArrayList<RedisListener>();

    protected String persistDir;

    protected long connectedClients;
    protected long totalConnectionsReceived;
    protected long totalCommandsProcessed;
    protected long clientLongestOutputList;
    protected long clientBiggestInputBuf;
    protected long blockedClients;
    protected long startTime;
    protected Map<Integer, Database> databases;
    protected int port;
    protected int maxDb;
    protected Persistifier persistifier;
    protected PortListener portListener;
    protected StartupThread startupThread;

    protected int threadPoolSize = DEFAULT_THREAD_POOL_SIZE;

    protected volatile boolean stopRequested;
}
