package examples;

import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.OS;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Administrator on 2016/8/26.
 */
public class test {

    public static final String MASTER_NAME = "master-1";

    protected static final Integer DEFAULT_PORT = 6379;
    protected static final Integer DEFAULT_SENTINEL_PORT = 26739;

    static RedisExecProvider redisExecProvider = RedisExecProvider.defaultProvider().override(OS.WINDOWS, "E:\\Program Files\\Redis\\redis-server.exe");

    protected static String host;

    public static void main(String[] args) throws Exception {
        System.out.println("sdfdfd");

        test2();
    }

    protected final static void createRedisSentinelInstance(final Integer sentinelPort, final Integer masterPort)
            throws Exception {
        System.out.println("Creating redis sentinel on port: " + sentinelPort);

        redis.embedded.RedisSentinel sentinel = redis.embedded.RedisSentinel.builder()
                .redisExecProvider(redisExecProvider)
                .port(sentinelPort)
                .setting(String.format("sentinel monitor %s %s %s 1", MASTER_NAME, host, DEFAULT_PORT))
                .setting(String.format("sentinel down-after-milliseconds %s 200", MASTER_NAME))
                .setting(String.format("sentinel failover-timeout %s 1000", MASTER_NAME))
                .build();

        //sentinels.put(sentinelPort, sentinel);
        System.out.println("Created embedded redis server on port " + sentinelPort);

        sentinel.start();

        System.out.println("started");

        sentinel.stop();
    }

    private static void test2() throws Exception {

        host = InetAddress.getLocalHost().getHostAddress();

        createRedisSentinelInstance(DEFAULT_SENTINEL_PORT, DEFAULT_PORT);



    }

    void test1() {
        File exe_file = new File("E:\\Program Files\\Redis\\redis-server.exe");
        RedisServer redisServer = new RedisServer(exe_file, 6380);
        redisServer.start();
    }
}
