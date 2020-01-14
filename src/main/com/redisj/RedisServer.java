package com.redisj;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
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
import java.util.LinkedHashSet;
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

    public static void main(String[] args) throws IOException {
        RedisServer server = new RedisServer().withPersistence();
        try {
            server.serveForEver(true);
            boolean ok = server.waitUntilAcceptingConnection(5000);
            if (ok) {
                server.logInfo("Press enter to stop.");
                @SuppressWarnings("unused")
                int key = System.in.read();
                System.exit(0);
            }

            server.logError("Timeout while waiting for server to accept connections");
            System.exit(2);
        }
        catch (BindException e) {
            server.logError("Failed to bind to port %d", server.port);
            System.exit(1);
        }
    }

    public static final int DEFAULT_THREAD_POOL_SIZE = 20;

    public static final int DEFAULT_PORT = 6379;

    public static final int DEFAULT_MAX_DB = 16;

    public static final String DEFAULT_WORKDIR = ".redisj";

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
        this.persistDir = createWorkDir();
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

    public RedisServer withVersion(String majorMinorRelease) {
        this.version = majorMinorRelease;
        return this;
    }

    public RedisServer serveForEver(boolean background) throws BindException {

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

        return this;
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

    public void persist() throws IOException {
        if (null!=persistifier) {
            persistifier.persist(databases);
        }
    }

    public void persist(int dbNum) throws IOException {
        if (null!=persistifier) {
            Database db = databases.get(dbNum);
            persistifier.persist(db);
        }
    }

    public void flushAll() {
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

    public boolean waitUntilAcceptingConnection(int timeoutMillis) {
        for (long expires = now()+timeoutMillis; now()<expires; ) {
            if (acceptingConnections) {
                return true;
            }
            try { Thread.sleep(100); } catch (InterruptedException e1) {}
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

    public String hset(int dbNum, String key, String field, String value) throws IOException {

        Worker worker = new Worker();
        Database db = worker._select(dbNum);

        Args args = new Args(key, field, value);
        worker.hset(db, args);
        String resp = worker.stream.toString();

        return resp;
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

    protected static String createWorkDir() {

        final String userHome = System.getProperty("user.home");
        final File workDir = new File(userHome, DEFAULT_WORKDIR);
        workDir.mkdirs();

        if (!workDir.isDirectory()) {
            throw new RuntimeException(String.format("%s is not a directory", workDir));
        }
        else if (!workDir.canWrite()) {
            throw new RuntimeException(String.format("%s is not a writable", workDir));
        }

        return workDir.getAbsolutePath();
    }

    protected boolean notExpired(Ageable a) {
        return null!=a && (a.expires<0 || a.expires<now());
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
            method = Worker.class.getDeclaredMethod(name, Database.class, Args.class);
        }
        catch (Exception e) {
            return null;
        }

        CommandMethod anno = null==method ? null : method.getAnnotation(CommandMethod.class);
        if (null==anno) {
            return null;
        }

        // If this server is supposed to stick to a given redis version,
        // check if the respective command would be supported by this redis version.
        if (version!=null) {
            if (version.compareTo(anno.since())<1) {
                return null;
            }
        }

        found = new WorkerMethod(name, anno, method);
        synchronized (methodCache) {
            methodCache.put(name, found);
        }
        return found;
    }

    class StartupThread extends Thread {

        public StartupThread() {
            super(StartupThread.class.getSimpleName());
            setDaemon(true);
        }

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

        public PortListener(int port) throws BindException {

            super(PortListener.class.getSimpleName());
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

        public boolean isBound() {
            return null!=socket && socket.isBound();
        }

        protected void serve() {

            boolean first = true;

            while (!stopRequested && null!=socket) {

                if (first) {
                    onServerStarting(port);
                }

                Socket clientSocket = null;
                try {
                    if (first) {
                        logInfo("%s[%d]: accepting connections", CN, port);
                        acceptingConnections = true;
                    }
                    clientSocket = socket.accept();

                    totalConnectionsReceived++;

                    Worker worker = new Worker(clientSocket);
                    workers.add(worker);
                    executor.execute(worker);
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

                first = false;
            }

            executor.shutdownNow();

            String reason = stopRequested ? "Requested" : isInterrupted() ? "Interrupted" : "Socket closed";

            stopRequested = false;
            onServerStopped(reason);
        }

        protected volatile ServerSocket socket;
        protected boolean stopRequested;
        protected int port;
        protected ExecutorService executor;
    }

    class WorkerMethod {

        public WorkerMethod(String name, CommandMethod anno, Method method) {
            this.name = name;
            this.db   = anno.db();
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

        void invoke(Worker worker, Args args) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
            Database db = null;
            try {
                if (this.db) {
                    db = worker.getSelectedDb();
                }
                method.invoke(worker, db, args);
            }
            finally {
                // TODO: unlock db
            }
        }

        private String name;
        private int min;
        boolean db;
        private int max;
        private int args;
        private boolean even;
        private boolean odd;
        //private RedisCommand anno;
        private Method method;
    }

    /**
     * This thread handles communication on a client socket.
     * I continues to read commands from the client and send replies until
     * the client finally disconnects.
     */
    class Worker implements Runnable {

        private String lastCommand;
        private OutputStream stream;

        public Worker(Socket clientSocket) {
            //super.setDaemon(true);
            this.socket = clientSocket;
            this.started = now();
        }

        private Worker() {
            this.socket = null;
            this.stream = new ByteArrayOutputStream();
            this.writer = new RESPWriter(this.stream);
        }

        @Override
        public void run() {

            try {
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
            }
            finally {
                workers.remove(this);
            }
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
                    if (null!=socket) {
                        socket.close();
                    }
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

                        if (!commandListeners.isEmpty()) {
                            Database db = getSelectedDb();
                            onBeforeCommand(db.number, db.size(), cmd, args);
                        }

                        dispatchCommand(cmd, args);

                        if (!commandListeners.isEmpty()) {
                            Database db = getSelectedDb();
                            onAfterCommand(db.number, db.size(), cmd, args);
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

            if (null!=socket) {
                socket.close();
            }
            socket = null;
        }

        public String getInfo() {
            String addr = socket.getRemoteSocketAddress().toString();
            if (addr.startsWith("/")) {
                addr = addr.substring(1);
            }
            long id   = clientId();
            int  fd   = socket.getLocalPort(); // no file descriptors in java
            long age  = now()-started;
            long idle = 0;
            int  db   = selectedDb;

            String flags = "N"; // TODO: what flag do exist?
            String cmd   = null==lastCommand ? "" : lastCommand;
            String name  = null==clientName ? "" : clientName;

            String info = String.format(
                    "id=%d addr=%s fd=%d name=%s age=%d idle=%d flags=%s db=%d sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=32768 obl=0 oll=0 omem=0 events=r cmd=%s",
                    id,    addr,   fd,   name,   age,   idle,   flags,   db, cmd
                    );
            return info;
        }

        protected void dispatchCommand(String cmd, Args args) throws IOException {

            try {
                this.lastCommand = cmd;

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

        @CommandMethod(args = {"db"}, since="1.0.0", db=false)
        protected void select(Database db, Args args) throws IOException {

            String num = args.get(0);
            _select(Integer.parseInt(num));

            if (maxDb>-1 && selectedDb>=maxDb) {
                writer.sendError("ERR", "Invalid database");
            }
            else {
                writer.write("+OK\r\n".getBytes());
            }
        }

        protected Database _select(int n) {
            selectedDb = n;
            return getSelectedDb();
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void dbsize(Database db, Args args) throws IOException {
            writer.sendNumber(db.size());
        }

        @CommandMethod(args = {"[ASYNC]"}, min=0, max=1, since="1.0.0", db=false)
        protected void flushall(Database db, Args args) throws IOException {
            synchronized (databases) {
                databases.clear();
            }
        }

        @CommandMethod(args = {"pattern"}, since="1.0.0", ro=true)
        protected void keys(Database db, Args args) throws IOException {

            String glob = args.get(0);

            Pattern rex = createRegexFromGlob(glob);
            ArrayList<String> matches = new ArrayList<String>();

            for (String key : db.keySet()) {
                if (rex.matcher(key).matches()) {
                    matches.add(key);
                }
            }

            writer.sendArray(matches);
        }

        @CommandMethod(args= {"key"}, since="1.0.0", ro=true)
        protected void get(Database db, Args args) throws IOException {
            String key = args.get(0);
            Ageable a = db.get(key, false);
            if (null==a) {
                writer.write(EMPTY_BYTES);
            }
            else {
                String string = a.get();
                writer.sendString(string);
            }
        }

        @CommandMethod(args= {"key"}, since="1.0.0", ro=true)
        protected void exists(Database db, Args args) throws IOException {
            String key = args.get(0);
            Ageable a = db.get(key, false);
            writer.sendNumber(null==a ? 0 : 1);
        }

        @CommandMethod(args= {"key"}, since="2.6.0")
        protected void bitcount(Database db, Args args) throws IOException {

            String key = args.get(0);
            int count = 0;
            Ageable a = db.get(key, false);
            if (null!=a) {
                String s = a.get();
                for (int i=0, len=s.length(); i<len; i++) {
                    char c = s.charAt(i);
                    int upper = ((byte)c) >> 4;
                    int lower = ((byte)c) & 0x0f;
                    count += NIBBLE_BITS[upper] + NIBBLE_BITS[lower];
                }
            }
            writer.sendNumber(count);
        }

        @CommandMethod(args= {"key", "offset"}, since="2.2.0", ro=true)
        protected void getbit(Database db, Args args) throws IOException {

            String key = args.get(0);
            int value = 0;
            Ageable a = db.get(key, false);
            if (null!=a) {
                String s = a.get();
                int off = Integer.parseInt(args.get(1));
                int pos = off/8;
                char c = (null==s || pos>=s.length()) ? 0 : s.charAt(pos);
                byte mask = (byte)(0x80 >> (off%8));
                if ((mask & c) > 0) {
                    value = 1;
                }
            }
            writer.sendNumber(value);
        }

        @CommandMethod(args = {"key", "value"}, since="1.0.0")
        protected void set(Database db, Args args) throws IOException {
            _set(db, args, false);
        }

        @CommandMethod(args = {"key", "value"}, since="1.0.0")
        protected void setnx(Database db, Args args) throws IOException {
            _set(db, args, true);
        }

        @CommandMethod(args = {"key", "value"}, since="2.0.0")
        protected void append(Database db, Args args) throws IOException {
            String key = args.get(0);
            String value = args.get(1);
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

        @CommandMethod(args = {"key1", "key2", "..."}, min=1, since="1.0.0", ro=true)
        protected void mget(Database db, Args args) throws IOException {
            writer.sendArrayLength(args.size());
            for (String key : args) {
                Ageable a = db.get(key, false);
                if (null==a) {
                    writer.write(EMPTY_BYTES);
                }
                else {
                    String string = a.get();
                    writer.sendString(string);
                }
            }
        }

        @CommandMethod(args = {"key"}, since="1.0.0", ro=true)
        protected void llen(Database db, Args args) throws IOException {

            String key = args.get(0);
            Ageable a = db.get(key, false);
            if (null==a) {
                writer.sendNumber(0);
            }
            else {
                List<Object> list = a.get();
                writer.sendNumber(list.size());
            }
        }

        @CommandMethod(args = {"key"}, since="1.0.0")
        protected void lpop(Database db, Args args) throws IOException {
            String key = args.get(0);
            _pop(db, key, true);
        }

        @CommandMethod(args = {"key"}, since="1.0.0")
        protected void rpop(Database db, Args args) throws IOException {
            String key = args.get(0);
            _pop(db, key, false);
        }

        @CommandMethod(args = {"key", "value"}, since="1.0.0")
        protected void rpush(Database db, Args args) throws IOException {
            String key   = args.get(0);
            String value = args.get(1);
            _push(db, key, value, false);
        }

        @CommandMethod(args = {"key", "value"}, since="1.0.0")
        protected void lpush(Database db, Args args) throws IOException {
            String key   = args.get(0);
            String value = args.get(1);
            _push(db, key, value, true);
        }

        @CommandMethod(args = {"key"}, since="1.0.0", ro=true)
        protected void type(Database db, Args args) throws IOException {

            String key = args.get(0);
            Ageable a = db.get(key, false);
            if (notExpired(a)) {
                Object value = a.value;
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

        @CommandMethod(args = {"key", "amout"}, since="2.6.0")
        protected void incrbyfloat(Database db, Args args) throws IOException {
            _incrDecr(db, args.get(0), true, args.get(1), true);
        }

        @CommandMethod(args = {"key", "amout"}, since="1.0.0")
        protected void decrby(Database db, Args args) throws IOException {
            _incrDecr(db, args.get(0), false, args.get(1), false);
        }

        @CommandMethod(args = {"key", "amout"}, since="1.0.0")
        protected void incrby(Database db, Args args) throws IOException {
            _incrDecr(db, args.get(0), true, args.get(1), false);
        }

        @CommandMethod(args = {"key"}, since="1.0.0")
        protected void decr(Database db, Args args) throws IOException {
            _incrDecr(db, args.get(0), false, "1", false);
        }

        @CommandMethod(args = {"key"}, since="1.0.0")
        protected void incr(Database db, Args args) throws IOException {
            _incrDecr(db, args.get(0), true, "1", false);
        }

        @CommandMethod(args = {"subcmd", "[option]"}, min=1, max=2, since="2.4.0")
        protected void client(Database db, Args args) throws IOException {
            String subcmd = args.get(0).toUpperCase();
            if ("ID".equals(subcmd)) {
                // since="5.0.0"
                writer.sendString(""+clientId());
            }
            else if ("LIST".equals(subcmd)) {
                // 2.4.0
                StringBuilder sb = new StringBuilder();
                synchronized (workers) {
                    for (Worker w : workers) {
                        sb.append(w.getInfo()).append("\r\n");
                    }
                }
                writer.sendString(sb.toString());
            }
            else if ("KILL".equals(subcmd)) {
                // 2.4.0
                String ipPort = args.get(1);
                synchronized (workers) {
                    for (Worker w : workers) {
                        if (w.hasAddr(ipPort)) {
                            w.kill();
                            break;
                        }
                    }
                }
            }
            else if ("GETNAME".equals(subcmd)) {
                // 2.6.9
                writer.sendString(clientName);
            }
            else if ("SETNAME".equals(subcmd)) {
                // 2.6.9
                assertArgCount(args, 2);
                clientName = args.get(1);
                writer.write(OK_BYTES);
            }
            else if ("PAUSE".equals(subcmd)) {
                // 2.9.50
                _todo("client PAUSE");
            }
            else if ("REPLY".equals(subcmd)) {
                // 3.2.
                _todo("client REPLY");
            }
            else if ("UNBLOCK".equals(subcmd)) {
                // 5.0.0.
                _todo("client UNBLOCK");
            }
            else {
                writer.sendError("ERR", "(error) ERR Syntax error, try CLIENT (ID | LIST | KILL ip:port | GETNAME | SETNAME connection-name)");
            }
        }

        @CommandMethod(args = {"message"}, since="1.0.0", ro=true)
        protected void echo(Database db, Args args) throws IOException {
            String message = args.get(0);
            writer.sendString(message);
        }

        @CommandMethod(args = {"key", "field", "value"}, since="2.0.0")
        protected void hset(Database db, Args args) throws IOException {
            String key = args.get(0);
            String field = args.get(1);
            String value = args.get(2);
            _hset(db, key, field, value, false);
        }

        @CommandMethod(args = {"key", "field", "value"}, since="2.0.0")
        protected void hsetnx(Database db, Args args) throws IOException {
            String key = args.get(0);
            String field = args.get(1);
            String value = args.get(2);
            _hset(db, key, field, value, true);
        }

        @CommandMethod(args = {"key", "field1", "field2", "field3", "..."}, min=2, since="2.0.0", ro=true)
        protected void hmget(Database db, Args args) throws IOException {
            String key = args.get(0);
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

        @CommandMethod(args = {"key", "field1", "value1", "field2", "value2", "..."}, odd=true, min=3, since="2.0.0")
        protected void hmset(Database db, Args args) throws IOException {
            String key = args.get(0);
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

        @CommandMethod(args = {"key"}, since="2.0.0", ro=true)
        protected void hkeys(Database db, Args args) throws IOException {
            String key = args.get(0);
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

        @CommandMethod(args = {"key"}, since="2.0.0", ro=true)
        protected void hvals(Database db, Args args) throws IOException {
            String key = args.get(0);
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

        @CommandMethod(args = {"key", "field1", "field2", "..."}, min=2, since="2.0.0")
        protected void hdel(Database db, Args args) throws IOException {

            Ageable a = db.get(args.key(), false);
            if (null!=a) {
                Hash hash = (Hash) a.value;
                int count = 0;
                for (int i=1; i<args.size(); i++) {
                    String field = args.get(i);
                    if (hash.contains(field)) {
                        hash.remove(field);
                        count++;
                    }
                }
                writer.sendNumber(count);
            }
        }

        @CommandMethod(args = {"key"}, since="2.0.0", ro=true)
        protected void hlen(Database db, Args args) throws IOException {
            String key = args.get(0);
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

        @CommandMethod(args = {"key", "field"}, since="3.2.0", ro=true)
        protected void hstrlen(Database db, Args args) throws IOException {

            String key   = args.get(0);
            String field = args.get(1);

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

        @CommandMethod(args = {"key", "field"}, since="2.0.0", ro=true)
        protected void hget(Database db, Args args) throws IOException {

            String key   = args.get(0);
            String field = args.get(1);

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

        @CommandMethod(args = {"key"}, since="2.0.0", ro=true)
        protected void hgetall(Database db, Args args) throws IOException {

            String key = args.get(0);

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

        @CommandMethod(args = {"key", "field", "amount"}, since="2.0.0")
        protected void hincrby(Database db, Args args) throws IOException {
            _hincrBy(db, args, false);
        }

        @CommandMethod(args = {"key", "field", "amount"}, since="2.6.0")
        protected void hincrbyfloat(Database db, Args args) throws IOException {
            _hincrBy(db, args, true);
        }

        @CommandMethod(args = {"key", "field"}, since="2.0.0", ro=true)
        protected void hexists(Database db, Args args) throws IOException {

            String key = args.get(0);
            String field = args.get(1);

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

        @CommandMethod(args = {}, since="1.0.0")
        protected void save(Database db, Args args) throws IOException {

            Persistifier pers = new Persistifier(RedisServer.this, "/tmp/radisj");
            pers.persist(databases);

            writer.write(OK_BYTES);
        }

        @CommandMethod(args = {}, min=0, max=1, since="1.0.0", ro=true)
        protected void ping(Database db, Args args) throws IOException {
            writer.sendString("PONG");
        }

        @CommandMethod(args = {"key1", "key2", "...", "timeout"}, min=2, since="2.0.0")
        protected void blpop(Database db, Args args) throws IOException {
            _bpop(db, true, args);
        }

        @CommandMethod(args = {"key"}, since="2.0.0")
        protected void brpop(Database db, Args args) throws IOException {
            _bpop(db, false, args);
        }

        @CommandMethod(args={"key"}, min=1, since="1.0.0")
        protected void del(Database db, Args args) throws IOException {
            String key = args.get(0);
            Ageable a = db.markDirty().remove(key);
            writer.sendNumber(notExpired(a) ? 1 : 0);
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void flushdb(Database db, Args args) throws IOException {
            db.markDirty().clear();
            writer.write(OK_BYTES);
        }

        @CommandMethod(args = {"key"}, since="1.0.0", ro=true)
        protected void ttl(Database db, Args args) throws IOException {

            String key = args.get(0);
            long ttl = -2;
            Ageable a = db.get(key, false);
            if (notExpired(a)) {
                if (a.expires<0) {
                    ttl = -1;
                }
                else {
                    ttl = (now()-a.expires)/1000;
                }
            }
            else {
                ttl = -2;
            }

            writer.sendNumber(ttl);
        }

        @CommandMethod(args = {"key"}, since="2.6.0", ro=true)
        protected void pttl(Database db, Args args) throws IOException {

            String key = args.get(0);
            long ttl = -2;
            Ageable a = db.get(key, false);
            if (notExpired(a)) {
                if (a.expires<0) {
                    ttl = -1;
                }
                else {
                    ttl = (now()-a.expires);
                }
            }
            else {
                ttl = -2;
            }

            writer.sendNumber(ttl);
        }

        @CommandMethod(args = {"key", "ttl"}, since="1.0.0")
        protected void expire(Database db, Args args) throws IOException {
            String key  = args.get(0);
            String ttl  = args.get(1);
            int    secs = Integer.parseInt(ttl);
            Ageable a = db.get(key, false);
            if (notExpired(a)) {
                a.expires = now()+1000*secs;
                writer.sendNumber(0);
            }
            else {
                writer.sendNumber(1);
            }
        }

        @CommandMethod(args = {"key", "ttl"}, since="1.0.0")
        protected void setex(Database db, Args args) throws IOException {
            String key   = args.get(0);
            int    secs  = Integer.parseInt(args.get(1));
            String value = args.get(2);
            Ageable a = db.get(key, false);
            if (null==a) {
                db.put(key, a = new Ageable(value));
                a.expires = now()+1000*secs;
                writer.sendNumber(1);
            }
            else {
                writer.sendNumber(0);
            }
        }

        @CommandMethod(args = {"key", "ttl"}, since="1.0.0")
        protected void psetex(Database db, Args args) throws IOException {
            String key   = args.get(0);
            int    millis  = Integer.parseInt(args.get(1));
            String value = args.get(2);
            Ageable a = db.get(key, false);
            if (null==a) {
                db.put(key, a = new Ageable(value));
                a.expires = now()+millis;
                writer.sendNumber(1);
            }
            else {
                writer.sendNumber(0);
            }
        }

        @CommandMethod(args = {"key1", "val1", "key2", "val2"}, min=2, even=true, since="1.0.1")
        protected void mset(Database db, Args args) throws IOException {
            _mset(db, args, false);
        }

        @CommandMethod(args = {"key1", "val1", "key2", "val2"}, min=2, even=true, since="1.0.1")
        protected void msetnx(Database db, Args args) throws IOException {
            _mset(db, args, true);
        }

        @CommandMethod(args = {}, db=false, since="1.0.0", ro=true)
        protected void info(Database unused, Args args) throws IOException {

            StringBuilder sb = new StringBuilder();

            long uptimeSeconds = (now()-startTime)/1000;
            int connectedClients = workers.size();

            sb.append("# Server\r\n");
            sb.append("redis_version:");
            sb.append(null==version ? "2.0.0" : version);
            sb.append(" ***** THIS IS NOT REAL REDIS BUT REDISJ - A VERY BASIC JAVA PORT *****\r\n");
            sb.append(String.format("tcp_port:%d\r\n", port));
            sb.append(String.format("uptime_in_seconds:%d\r\n", uptimeSeconds));
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
                keys = new LinkedHashSet<Integer>(databases.keySet());
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

        @CommandMethod(args = {"key", "cursor", "[MATCH pattern]"}, min=2, since="2.8.0")
        protected void hscan(Database db, Args args) throws IOException {
            _todo("hscan");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void monitor(Database db, Args args) throws IOException {
            _todo("monitor");
        }

        @CommandMethod(args = {"[NOSAVE|SAVE]"}, min=0, max=1, since="1.0.0")
        protected void shutdown(Database db, Args args) throws IOException {

            logInfo("User requested shutdown...");
            stopRequested = true;
            portListener.executor.shutdownNow();
            portListener.interrupt();

            for (Worker w : workers) {
                w.kill();
            }
            if (null!=persistifier) {
                try {
                    logInfo("Saving the final RDB snapshot before exiting.");
                    persistifier.persist(databases);
                }
                catch (Exception e) {
                    logError("Exception: %s", e);
                }
            }
            onServerStopped("USERREQUEST");
            stop();
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void lastsave(Database db, Args args) throws IOException {
            _todo("lastsave");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sync(Database db, Args args) throws IOException {
            _todo("sync");
        }

        @CommandMethod(args = {}, since="2.6.0", ro=true)
        protected void time(Database db, Args args) throws IOException {
            long millis = now();
            ArrayList<String> list = new ArrayList<String>();
            list.add(Long.toString(millis/1000));
            list.add(Long.toString(millis%1000) + "000");
            writer.sendArray(list);
        }

        @CommandMethod(args= {"key", "bit", "value"}, since="2.2.0")
        protected void setbit(Database db, Args args) throws IOException {
            _todo("setbit");
        }

        @CommandMethod(args = {"key", "value"}, since="2.2.0")
        protected void rpushx(Database db, Args args) throws IOException {
            _todo("rpushx");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void auth(Database db, Args args) throws IOException {
            _todo("auth");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void bgrewriteaof(Database db, Args args) throws IOException {
            _todo("bgrewriteaof");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void bgsave(Database db, Args args) throws IOException {
            _todo("bgsave");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void bitfield(Database db, Args args) throws IOException {
            _todo("bitfield");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void bitop(Database db, Args args) throws IOException {
            _todo("bitop");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void bitpos(Database db, Args args) throws IOException {
            _todo("bitpos");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void brpoplpush(Database db, Args args) throws IOException {
            _todo("brpoplpush");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void bzpopmin(Database db, Args args) throws IOException {
            _todo("bzpopmin");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void bzpopmax(Database db, Args args) throws IOException {
            _todo("bzpopmax");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void cluster(Database db, Args args) throws IOException {
            _todo("cluster");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void command(Database db, Args args) throws IOException {
            _todo("command");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void config(Database db, Args args) throws IOException {
            _todo("config");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void debug(Database db, Args args) throws IOException {
            _todo("debug");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void discard(Database db, Args args) throws IOException {
            _todo("discard");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void dump(Database db, Args args) throws IOException {
            _todo("dump");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void eval(Database db, Args args) throws IOException {
            _todo("eval");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void evalsha(Database db, Args args) throws IOException {
            _todo("evalsha");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void exec(Database db, Args args) throws IOException {
            _todo("exec");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void expireat(Database db, Args args) throws IOException {
            _todo("expireat");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void geoadd(Database db, Args args) throws IOException {
            _todo("geoadd");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void geohash(Database db, Args args) throws IOException {
            _todo("geohash");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void geopos(Database db, Args args) throws IOException {
            _todo("geopos");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void geodist(Database db, Args args) throws IOException {
            _todo("geodist");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void georadius(Database db, Args args) throws IOException {
            _todo("georadius");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void georadiusbymember(Database db, Args args) throws IOException {
            _todo("georadiusbymember");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void getrange(Database db, Args args) throws IOException {
            _todo("getrange");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void getset(Database db, Args args) throws IOException {
            _todo("getset");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void lolwut(Database db, Args args) throws IOException {
            _todo("lolwut");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void lindex(Database db, Args args) throws IOException {
            _todo("lindex");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void linsert(Database db, Args args) throws IOException {
            _todo("linsert");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void lpushx(Database db, Args args) throws IOException {
            _todo("lpushx");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void lrange(Database db, Args args) throws IOException {
            _todo("lrange");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void lrem(Database db, Args args) throws IOException {
            _todo("lrem");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void lset(Database db, Args args) throws IOException {
            _todo("lset");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void ltrim(Database db, Args args) throws IOException {
            _todo("ltrim");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void memory(Database db, Args args) throws IOException {
            _todo("memory");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void migrate(Database db, Args args) throws IOException {
            _todo("migrate");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void module(Database db, Args args) throws IOException {
            _todo("module");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void move(Database db, Args args) throws IOException {
            _todo("move");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void multi(Database db, Args args) throws IOException {
            _todo("multi");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void object(Database db, Args args) throws IOException {
            _todo("object");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void persist(Database db, Args args) throws IOException {
            _todo("persist");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void pexpire(Database db, Args args) throws IOException {
            _todo("pexpire");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void pexpireat(Database db, Args args) throws IOException {
            _todo("pexpireat");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void pfadd(Database db, Args args) throws IOException {
            _todo("pfadd");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void pfcount(Database db, Args args) throws IOException {
            _todo("pfcount");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void pfmerge(Database db, Args args) throws IOException {
            _todo("pfmerge");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void psubscribe(Database db, Args args) throws IOException {
            _todo("psubscribe");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void pubsub(Database db, Args args) throws IOException {
            _todo("pubsub");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void publish(Database db, Args args) throws IOException {
            _todo("publish");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void punsubscribe(Database db, Args args) throws IOException {
            _todo("punsubscribe");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void quit(Database db, Args args) throws IOException {
            _todo("quit");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void randomkey(Database db, Args args) throws IOException {
            _todo("randomkey");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void readonly(Database db, Args args) throws IOException {
            _todo("readonly");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void readwrite(Database db, Args args) throws IOException {
            _todo("readwrite");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void rename(Database db, Args args) throws IOException {
            _todo("rename");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void renamenx(Database db, Args args) throws IOException {
            _todo("renamenx");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void restore(Database db, Args args) throws IOException {
            _todo("restore");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void role(Database db, Args args) throws IOException {
            @SuppressWarnings("serial")
            ArrayList<String> list = new ArrayList<String>() {
                { add("master"); add("0"); add(null); }
            };
            writer.sendArray(list);
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void rpoplpush(Database db, Args args) throws IOException {
            _todo("rpoplpush");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sadd(Database db, Args args) throws IOException {
            _todo("sadd");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void scard(Database db, Args args) throws IOException {
            _todo("scard");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void script(Database db, Args args) throws IOException {
            _todo("script");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sdiff(Database db, Args args) throws IOException {
            _todo("sdiff");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sdiffstore(Database db, Args args) throws IOException {
            _todo("sdiffstore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void setrange(Database db, Args args) throws IOException {
            _todo("setrange");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sinter(Database db, Args args) throws IOException {
            _todo("sinter");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sinterstore(Database db, Args args) throws IOException {
            _todo("sinterstore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sismember(Database db, Args args) throws IOException {
            _todo("sismember");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void slaveof(Database db, Args args) throws IOException {
            _todo("slaveof");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void replicaof(Database db, Args args) throws IOException {
            _todo("replicaof");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void slowlog(Database db, Args args) throws IOException {
            _todo("slowlog");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void smembers(Database db, Args args) throws IOException {
            _todo("smembers");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void smove(Database db, Args args) throws IOException {
            _todo("smove");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sort(Database db, Args args) throws IOException {
            _todo("sort");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void spop(Database db, Args args) throws IOException {
            _todo("spop");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void srandmember(Database db, Args args) throws IOException {
            _todo("srandmember");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void srem(Database db, Args args) throws IOException {
            _todo("srem");
        }

        @CommandMethod(args = {}, since="1.0.0", ro=true)
        protected void strlen(Database db, Args args) throws IOException {
            Ageable a = db.get(args.key(), false);
            writer.sendNumber(null==a ? 0 : ((String)a.get()).length());
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void subscribe(Database db, Args args) throws IOException {
            _todo("subscribe");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sunion(Database db, Args args) throws IOException {
            _todo("sunion");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sunionstore(Database db, Args args) throws IOException {
            _todo("sunionstore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void swapdb(Database db, Args args) throws IOException {
            _todo("swapdb");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void psync(Database db, Args args) throws IOException {
            _todo("psync");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void touch(Database db, Args args) throws IOException {
            _todo("touch");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void unsubscribe(Database db, Args args) throws IOException {
            _todo("unsubscribe");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void unlink(Database db, Args args) throws IOException {
            _todo("unlink");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void unwatch(Database db, Args args) throws IOException {
            _todo("unwatch");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void wait(Database db, Args args) throws IOException {
            _todo("wait");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void watch(Database db, Args args) throws IOException {
            _todo("watch");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zadd(Database db, Args args) throws IOException {
            _todo("zadd");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zcard(Database db, Args args) throws IOException {
            _todo("zcard");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zcount(Database db, Args args) throws IOException {
            _todo("zcount");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zincrby(Database db, Args args) throws IOException {
            _todo("zincrby");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zinterstore(Database db, Args args) throws IOException {
            _todo("zinterstore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zlexcount(Database db, Args args) throws IOException {
            _todo("zlexcount");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zpopmax(Database db, Args args) throws IOException {
            _todo("zpopmax");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zpopmin(Database db, Args args) throws IOException {
            _todo("zpopmin");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrange(Database db, Args args) throws IOException {
            _todo("zrange");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrangebylex(Database db, Args args) throws IOException {
            _todo("zrangebylex");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrevrangebylex(Database db, Args args) throws IOException {
            _todo("zrevrangebylex");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrangebyscore(Database db, Args args) throws IOException {
            _todo("zrangebyscore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrank(Database db, Args args) throws IOException {
            _todo("zrank");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrem(Database db, Args args) throws IOException {
            _todo("zrem");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zremrangebylex(Database db, Args args) throws IOException {
            _todo("zremrangebylex");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zremrangebyrank(Database db, Args args) throws IOException {
            _todo("zremrangebyrank");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zremrangebyscore(Database db, Args args) throws IOException {
            _todo("zremrangebyscore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrevrange(Database db, Args args) throws IOException {
            _todo("zrevrange");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrevrangebyscore(Database db, Args args) throws IOException {
            _todo("zrevrangebyscore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zrevrank(Database db, Args args) throws IOException {
            _todo("zrevrank");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zscore(Database db, Args args) throws IOException {
            _todo("zscore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zunionstore(Database db, Args args) throws IOException {
            _todo("zunionstore");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void scan(Database db, Args args) throws IOException {
            _todo("scan");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void sscan(Database db, Args args) throws IOException {
            _todo("sscan");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void zscan(Database db, Args args) throws IOException {
            _todo("zscan");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xinfo(Database db, Args args) throws IOException {
            _todo("xinfo");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xadd(Database db, Args args) throws IOException {
            _todo("xadd");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xtrim(Database db, Args args) throws IOException {
            _todo("xtrim");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xdel(Database db, Args args) throws IOException {
            _todo("xdel");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xrange(Database db, Args args) throws IOException {
            _todo("xrange");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xrevrange(Database db, Args args) throws IOException {
            _todo("xrevrange");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xlen(Database db, Args args) throws IOException {
            _todo("xlen");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xread(Database db, Args args) throws IOException {
            _todo("xread");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xgroup(Database db, Args args) throws IOException {
            _todo("xgroup");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xreadgroup(Database db, Args args) throws IOException {
            _todo("xreadgroup");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xack(Database db, Args args) throws IOException {
            _todo("xack");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xclaim(Database db, Args args) throws IOException {
            _todo("xclaim");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void xpending(Database db, Args args) throws IOException {
            _todo("xpending");
        }

        @CommandMethod(args = {}, since="1.0.0")
        protected void latency(Database db, Args args) throws IOException {
            _todo("latency");
        }

        protected void _todo(String cmd) throws IOException {
            writer.sendError("TODO", "Command '%s' not yet implemented", cmd);
        }

        protected void _set(Database db, List<String> args, boolean nx) throws IOException {

            assertArgCount(args, 2);
            String key  = args.get(0);
            String value = args.get(1);

            if (nx) {
                if (db.containsKey(key)) {
                    writer.sendError("ERR", "Key exists");
                    return;
                }
            }

            db.put(key, new Ageable(value));
            db.markDirty();
            writer.write(OK_BYTES);
        }

        protected void _mset(Database db, List<String> args, boolean nx) throws IOException {

            db.markDirty();
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

        protected void _incrDecr(Database db, String key, boolean incr, String amount, boolean _float) throws IOException {

            db.markDirty();
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

        protected void _hincrBy(Database db, Args args, boolean _float) throws IOException {

            String key   = args.get(0);
            String field = args.get(1);

            synchronized (db) {
                Ageable a = db.get(key, false);
                Hash hash = null;
                if (null==a) {
                    db.put(key, new Ageable(hash = new Hash()));
                }
                else {
                    hash = (Hash) a.value;
                }

                if (_float) {
                    double incr = Double.parseDouble(args.get(2));
                    String value = hash.getOrDefault(field, "0");
                    double sum = Double.parseDouble(value)+incr;
                    String string = Double.toString(sum);
                    hash.put(field, string);
                    writer.sendString(string);
                }
                else {
                    long incr = Long.parseLong(args.get(2));
                    String value = hash.getOrDefault(field, "0");
                    long sum = Long.parseLong(value)+incr;
                    String string = Long.toString(sum);
                    hash.put(field, string);
                    writer.sendNumber(sum);
                }
            }
        }

        protected void _bpop(Database db, boolean left, List<String> args) throws IOException {

            String last = args.get(args.size()-1);
            Long timeout = toLong(last);
            long expires = (timeout>0) ? now()+1000*timeout : -1;

            db.markDirty();
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

        protected void _pop(Database db, String key, boolean left) throws IOException {
            db.markDirty();
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

        protected void _push(Database db, String key, String val, boolean left) throws IOException {
            Ageable a = db.get(key, false);
            if (null==a) {
                db.put(key, a = new Ageable(new ArrayList<Object>()));
                db.markDirty();
            }

            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) a.value;
            db.markDirty();
            if (left) {
                list.add(0, val);
            }
            else {
                list.add(val); /// at end
            }
            writer.sendNumber(list.size());
        }

        protected void _hset(Database db, String key, String field, String value, boolean nx) throws IOException {

            synchronized (db) {
                Ageable a = db.get(key, false);
                Hash hash = null;
                if (null==a) {
                    db.markDirty();
                    db.put(key, new Ageable(hash = new Hash()));
                }
                else {
                    hash = (Hash) a.value;
                }

                if (nx && hash.contains(field)) {
                    writer.sendNumber(0);
                }
                else {
                    db.markDirty();
                    hash.put(field, value);
                    writer.sendNumber(1);
                }
            }

        }

        protected void kill() {
            try {
                socket.close();
                socket = null;
            }
            catch (Exception e) {
                logError("%s[%d]: kill: %s, ", getClass().getSimpleName(), port, e);
            }
        }

        protected boolean hasAddr(String ipPort) {
            String actual = socket.getRemoteSocketAddress().toString();
            if (actual.startsWith("/")) {
                actual = actual.substring(1);
            }
            return ipPort.equals(actual);
        }

        protected long clientId() {
            return Thread.currentThread().getId();
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

        protected String clientName;
        protected int selectedDb;
        protected RESPReader reader;
        protected long started;

        protected RESPWriter writer;
        protected Socket socket;
        final int NIBBLE_BITS[] = {
                0, 1, 1, 2, 1, 2, 2, 3,
                1, 2, 2, 3, 2, 3, 3, 4
        };
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
            else if (line.startsWith("#")) {
                return readHash(count);
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
                logInfo("RESPReader[%d]: client disconnected %s", port, addr);
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

        protected Hash readHash(int count) throws IOException {
            Hash hash = new Hash(count);
            for (int i=0; i<count; i+=2) {
                String key  = readString();
                String value = readString();
                hash.put(key, value);
            }
            return hash;
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

        public void sendHash(Hash hash) throws IOException {
            sendHashLength(hash.size());
            for (Entry<String, String> e : hash.entrySet()) {
                sendString(e.getKey());
                sendString(e.getValue());
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

        public void sendHashLength(int len) throws IOException {
            output.write(("#" + len).getBytes());
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
            logError(CN + ".RESPWriter.sendError: " + formatted);
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
            this(redisServer, createWorkDir());
        }

        public Persistifier(RedisServer redisServer, String dirname) {
            super(Persistifier.class.getSimpleName());
            super.setDaemon(true);
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
                    else if (obj instanceof Hash) {
                        Hash hash = (Hash) obj;
                        writer.sendString(key);
                        writer.sendNumber(a.expires);
                        writer.sendHash(hash);
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

        public Hash(int count) {
            super(count);
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

        public Args(String ... strings) {
            if (null!=strings) {
                for (String s : strings) {
                    add(s);
                }
            }
        }

        public String key() {
            return get(0);
        }

    }

    /**
     * We need this annotation at runtime to find methods matching redis command:
     */
    @Retention( RetentionPolicy.RUNTIME )
    public @interface CommandMethod {

        /**
         * List of arguments required for this command, mainly for testing number of arguments
         * but also for generating usage information.
         * @return
         */
        String[] args();

        /**
         * true if command required an even number of arguments
         * @return
         */
        boolean even() default false;

        /**
         * true if command required an odd number of arguments
         * @return
         */
        boolean odd() default false;

        /**
         * true if the command requires an instance of the currently selected database
         * @return
         */
        boolean db() default true;

        /**
         * Minimum number of arguments needed if >-1
         * @return
         */
        int min() default -1;

        /**
         * Maximum number of arguments needed if >-1
         * @return
         */
        int max() default -1;

        String since();

        // TODO: Use info whether a methoid is R/O only.
        boolean ro() default false;
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

    protected LinkedHashSet<Worker> workers = new LinkedHashSet<Worker>();

    protected volatile boolean acceptingConnections = false;
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
    protected String version;
}
