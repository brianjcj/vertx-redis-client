package io.vertx.test.redis;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.RedisSentinel;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * This test relies on a Redis server, by default it will start and stop a Redis server
 */
public class RedisClientBackedBySentinelTest extends RedisSentinelClientTestBase {

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // blocking for 1 sec for sentinel to settle down and detect master and slaves
        CountDownLatch latch = new CountDownLatch(1);
        Handler<Future<Void>> blockingCodeHandler = future -> {
            // Non event loop
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                //ignoring it
            } finally {
                future.complete();
            }
        };

        vertx.executeBlocking(blockingCodeHandler, asyncResult -> {
            redisSentinel = RedisSentinel.create(vertx, getSentinelConfig());
            latch.countDown();
        });

        awaitLatch(latch);

    }

    @Override
    public void tearDown() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        redisSentinel.close(asyncResult -> {
            if (asyncResult.succeeded()) {
                latch.countDown();
            } else {
                throw new RuntimeException("failed to setup", asyncResult.cause());
            }
        });
        awaitLatch(latch);
        super.tearDown();

    }


    @Test
    public void testRedisClientConnectViaSentinel() {

        JsonArray ja = new JsonArray();
        for (int i = 0; i < 3; ++i) {
            ja.add(new JsonObject().put("host", host).put("port", DEFAULT_SENTINEL_PORT + i - 1));
        }

        System.out.println("sentinels: " + ja);

        RedisClient redisClient = RedisClient.create(vertx,
                new RedisOptions().setMaster(MASTER_NAME).setSentinels(ja));

        redisClient.set("jcj", "ivy", result -> {
            assertTrue(result.succeeded());
            redisClient.get("jcj", result2 -> {
                System.out.println("get: " + result2.result());
                assertTrue(result2.succeeded());
                assertTrue(result2.result().equals("ivy"));

                instances.get(DEFAULT_PORT).stop();
                //instances.get(DEFAULT_PORT).start();

                vertx.setTimer(10, aLong -> {
                    redisClient.get("jcj", result3 -> {
                        System.out.println("get: " + result3.result());
                        assertTrue(result3.succeeded());
                        assertTrue(result2.result().equals("ivy"));
                        testComplete();
                    });
                });
            });
        });

        await();
    }
}
