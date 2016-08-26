package io.vertx.test.redis;

import redis.embedded.RedisExecProvider;
import redis.embedded.util.OS;

import java.io.File;

/**
 * Created by Administrator on 2016/8/26.
 */
public class TestEnvConfig {
    static RedisExecProvider redisExecProvider = RedisExecProvider.defaultProvider().override(OS.WINDOWS, "E:\\Program Files\\Redis\\redis-server.exe");
}
