package io.vertx.redis;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

/**
 * Created by Administrator on 2016/8/26.
 */
public interface BaseRedisClient {

    /**
     * Close the client - when it is fully closed the handler will be called.
     *
     * @param handler
     */
    void close(Handler<AsyncResult<Void>> handler);

}
