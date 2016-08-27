package io.vertx.test.redis;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.RedisSentinel;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * This test relies on a Redis server, by default it will start and stop a Redis server
 */
public class RedisSentinelTest extends RedisSentinelClientTestBase {

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
    @Ignore //embedded redis doesn't support flushConfig command
    public void testFlushConfig() {
        redisSentinel.flushConfig(reply -> {
            assertTrue(reply.succeeded());
            if (reply.succeeded()) {
                reply.result();
                testComplete();
            }
        });
        await();
    }

    public static JsonObject convertToJsonObject(JsonArray array) {
        JsonObject object = new JsonObject();
        for (int fieldCount = 0; fieldCount < array.size(); fieldCount += 2) {
            object.put(array.getString(fieldCount), array.getString(fieldCount + 1));
        }
        return object;
    }

    @Test
    public void testMasters() {
        redisSentinel.masters(reply -> {
            assertTrue(reply.succeeded());
            assertEquals(1, reply.result().size());
            JsonObject result = convertToJsonObject(reply.result().getJsonArray(0));
            assertEquals(MASTER_NAME, result.getString("name"));
            assertEquals(host, result.getString("ip"));
            System.out.println("master port: " + result.getString("port"));
            assertEquals(String.valueOf(DEFAULT_PORT), result.getString("port"));
            testComplete();
        });
        await();
    }

    @Test
    public void testMaster() {
        redisSentinel.master(MASTER_NAME, reply -> {
            System.out.println("master: " + reply.result());
            assertTrue(reply.succeeded());
            testComplete();
        });
        await();
    }

    @Test
    public void testSlaves() {
        redisSentinel.slaves(MASTER_NAME, reply -> {
            System.out.println("slaves: " + reply.result());
            assertTrue(reply.succeeded());
            testComplete();
        });
        await();
    }

    @Test
    public void testSentinels() {
        redisSentinel.sentinels(MASTER_NAME, reply -> {
            assertTrue(reply.succeeded());
            testComplete();
        });
        await();
    }

    @Test
    public void testMasterAddrByName() {
        redisSentinel.getMasterAddrByName(MASTER_NAME, reply -> {
            System.out.println("master address: " + reply.result());
            assertTrue(Integer.parseInt(reply.result().getString(1)) == DEFAULT_PORT);
            assertTrue(reply.result().getString(0).equals(host));
            assertTrue(reply.succeeded());
            testComplete();
        });
        await();
    }

    @Test
    public void testReset() {
        redisSentinel.reset(MASTER_NAME, reply -> {
            assertTrue(reply.succeeded());
            testComplete();
        });
        await();
    }

    @Test
    @Ignore // ignoring as test is flaky, sentinel does not discovery slave
    public void testFailover() {
        redisSentinel.failover(MASTER_NAME, reply -> {
            assertTrue(reply.succeeded());
            assertEquals("OK", reply.result().toString());
            testComplete();
        });
        await();
    }

    @Test
    @Ignore // ignoring as test is flaky, sentinel does not discovery slave or is subjected to timing
    public void testCkquorum() {
        redisSentinel.failover(MASTER_NAME, reply -> {
            assertTrue(reply.succeeded());
            assertEquals("OK", reply.result().toString());
            testComplete();
        });
        await();
    }
}
