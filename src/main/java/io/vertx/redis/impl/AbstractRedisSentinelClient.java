package io.vertx.redis.impl;

import io.vertx.core.Vertx;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.RedisSentinel;

/**
 * Abstract Redis Sentinel Client.
 */
public abstract class AbstractRedisSentinelClient extends AbstractRedisClient implements RedisSentinel {


    AbstractRedisSentinelClient(Vertx vertx, RedisOptions config) {
        super(vertx, config);
    }

    protected ResponseTransform getResponseTransformFor(RedisCommand command) {
        if (command == RedisCommand.SENTINEL_INFO) {
            return ResponseTransform.INFO;
        }

        return super.getResponseTransformFor(command);
    }
}