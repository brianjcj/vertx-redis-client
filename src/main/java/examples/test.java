package examples;

import redis.embedded.RedisServer;

import java.io.File;
import java.io.IOException;

/**
 * Created by Administrator on 2016/8/26.
 */
public class test {

    public static void main(String[] args) throws IOException {
        System.out.println("sdfdfd");

        File exe_file = new File("E:\\Program Files\\Redis\\redis-server.exe");
        RedisServer redisServer = new RedisServer(exe_file, 6380);
        redisServer.start();
    }
}
