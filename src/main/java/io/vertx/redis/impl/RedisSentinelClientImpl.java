package io.vertx.redis.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.RedisSentinel;

import static io.vertx.redis.impl.RedisCommand.*;

/**
 * Implementation of {@link AbstractRedisSentinelClient}
 */
public class RedisSentinelClientImpl extends AbstractRedisSentinelClient {
    public RedisSentinelClientImpl(Vertx vertx, RedisOptions config) {
        super(vertx, config);
    }

    @Override
    public RedisSentinel masters(Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(SENTINEL_MASTERS, null, handler);
        return this;
    }

    @Override
    public RedisSentinel master(String name, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(SENTINEL_MASTER, toPayload(name), handler);
        return this;
    }

    @Override
    public RedisSentinel slaves(String name, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(SENTINEL_SLAVES, toPayload(name), handler);
        return this;
    }

    @Override
    public RedisSentinel sentinels(String name, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(SENTINEL_SENTINELS, toPayload(name), handler);
        return this;
    }

    @Override
    public RedisSentinel getMasterAddrByName(String name, Handler<AsyncResult<JsonArray>> handler) {
        sendJsonArray(SENTINEL_GET_MASTER_ADDR_BY_NAME, toPayload(name), handler);
        return this;
    }

    @Override
    public RedisSentinel reset(String pattern, Handler<AsyncResult<Void>> handler) {
        sendVoid(SENTINEL_RESET, toPayload(pattern), handler);
        return this;
    }

    @Override
    public RedisSentinel failover(String name, Handler<AsyncResult<String>> handler) {
        sendString(SENTINEL_FAILOVER, toPayload(name), handler);
        return this;
    }

    @Override
    public RedisSentinel ckquorum(String name, Handler<AsyncResult<String>> handler) {
        sendString(SENTINEL_CKQUORUM, toPayload(name), handler);
        return this;
    }

    @Override
    public RedisSentinel flushConfig(Handler<AsyncResult<Void>> handler) {
        sendVoid(SENTINEL_FLUSHCONFIG, null, handler);
        return this;
    }
}
