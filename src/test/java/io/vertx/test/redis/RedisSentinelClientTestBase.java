package io.vertx.test.redis;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.RedisSentinel;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import redis.embedded.RedisServer;

import java.net.InetAddress;
import java.util.Map;


public abstract class RedisSentinelClientTestBase extends AbstractRedisSentinelBase {
    protected RedisSentinel redisSentinel;

    @BeforeClass
    static public void startRedis() throws Exception {

        /**
         * Test setup is
         *
         * Sentinel S1 -> Redis Master M
         * Sentinel S2 -> Redis Master M
         * Sentinel S3 -> Redis Master M
         *
         * Redis Master M -> Redis Slave S
         */

        // redis slave <-> master sync doesn't work with 127.0.0.1/localhost.
        // hence getting ip address.
        host = InetAddress.getLocalHost().getHostAddress();

        // create Redis Master
        createRedisInstance(DEFAULT_PORT, "loglevel debug");
        instances.get(DEFAULT_PORT).start();

        // create Redis Slave
        createSlaveRedisInstance(DEFAULT_PORT + 1, DEFAULT_PORT, "loglevel debug");
        instances.get(DEFAULT_PORT + 1).start();

        // create sentinels
        for (int i = 0; i < 3 ; i++) {
            createRedisSentinelInstance(DEFAULT_SENTINEL_PORT + i, DEFAULT_PORT);
            sentinels.get(DEFAULT_SENTINEL_PORT + i).start();

        }

    }

    @AfterClass
    static public void stopRedis() throws Exception {
        // stop sentinels
        for (Map.Entry<Integer, redis.embedded.RedisSentinel> entry : sentinels.entrySet()) {
            if (entry != null) {
                entry.getValue().stop();
            }
        }

        // stop redis instances
        for (Map.Entry<Integer, RedisServer> entry : instances.entrySet()) {
            if (entry != null) {
                entry.getValue().stop();
            }
        }

    }

    protected RedisOptions getSentinelConfig() {
       RedisOptions config = new RedisOptions();
        config.setHost(host);
        config.setPort(DEFAULT_SENTINEL_PORT);
        return config;
    }


}
