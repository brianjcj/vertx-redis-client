/**
 * Copyright 2015 Red Hat, Inc.
 *
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Eclipse Public License v1.0
 *  and Apache License v2.0 which accompanies this distribution.
 *
 *  The Eclipse Public License is available at
 *  http://www.eclipse.org/legal/epl-v10.html
 *
 *  The Apache License v2.0 is available at
 *  http://www.opensource.org/licenses/apache2.0.php
 *
 *  You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.redis.impl;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.BaseRedisClient;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public abstract class AbstractRedisClient implements BaseRedisClient {

  private final EventBus eb;
  private final RedisSubscriptions subscriptions;
  private final String encoding;
  private final Charset charset;
  private final Charset binaryCharset;
  private final String baseAddress;

  // we need 2 connections, one for normal commands and a second in case we do pub/sub
  private final RedisConnection redis;
  private final RedisConnection pubsub;

  AbstractRedisClient(Vertx vertx, RedisOptions config) {
    this.eb = vertx.eventBus();
    this.encoding = config.getEncoding();
    this.charset = Charset.forName(encoding);
    this.binaryCharset = Charset.forName("iso-8859-1");
    this.baseAddress = config.getAddress();

    subscriptions = new RedisSubscriptions(vertx);

    redis = new RedisConnection(vertx, config, null);
    pubsub = new RedisConnection(vertx, config, subscriptions);
  }

  @Override
  public synchronized void close(Handler<AsyncResult<Void>> handler) {
    // this is a special case it should sent the message QUIT and then close the sockets
    final AtomicInteger cnt = new AtomicInteger(0);

    final Handler<AsyncResult<Void>> cb = v -> {
      if (cnt.incrementAndGet() == 2) {
        handler.handle(Future.succeededFuture());
      }
    };

    redis.disconnect(cb);
    pubsub.disconnect(cb);
  }

  protected ResponseTransform getResponseTransformFor(RedisCommand command) {
    if (command == RedisCommand.HGETALL) {
      return ResponseTransform.HASH;
    }
    if (command == RedisCommand.INFO) {
      return ResponseTransform.INFO;
    }

    if (command == RedisCommand.EVAL || command == RedisCommand.EVALSHA) {
      return ResponseTransform.ARRAY;
    }

    return ResponseTransform.NONE;
  }

  final void sendString(final RedisCommand command, final List<?> args, final Handler<AsyncResult<String>> resultHandler) {
    send(command, args, String.class, false, resultHandler);
  }

  final void sendLong(final RedisCommand command, final List<?> args, final Handler<AsyncResult<Long>> resultHandler) {
    send(command, args, Long.class, false, resultHandler);
  }

  final void sendVoid(final RedisCommand command, final List<?> args, final Handler<AsyncResult<Void>> resultHandler) {
    send(command, args, Void.class, false, resultHandler);
  }

  final void sendJsonArray(final RedisCommand command, final List<?> args, final Handler<AsyncResult<JsonArray>> resultHandler) {
    send(command, args, JsonArray.class, false, resultHandler);
  }

  final void sendJsonObject(final RedisCommand command, final List<?> args, final Handler<AsyncResult<JsonObject>> resultHandler) {
    send(command, args, JsonObject.class, false, resultHandler);
  }

  final <T> void send(final RedisCommand command, final List<?> redisArgs, final Class<T> returnType,
                      final boolean binary,
                      final Handler<AsyncResult<T>> resultHandler) {

    final Command<T> cmd = new Command<>(Vertx.currentContext(), command, redisArgs, binary ? binaryCharset : charset, getResponseTransformFor(command), returnType).handler(resultHandler);

    switch (command) {
      case PSUBSCRIBE:
        cmd.setExpectedReplies(redisArgs.size());

        for (Object obj : redisArgs) {
          String pattern = (String) obj;
          // compose the listening address as base + . + pattern
          final String vertxChannel = baseAddress + "." + pattern;
          subscriptions.registerPatternSubscribeHandler(pattern, (pattern1, replyData) -> {
            JsonObject replyMessage = new JsonObject();
            replyMessage.put("status", "ok");
            JsonObject message = new JsonObject();
            message.put("pattern", pattern1);
            message.put("channel", replyData[2].asType(String.class, encoding));
            message.put("message", replyData[3].asType(String.class, encoding));
            replyMessage.put("value", message);
            eb.send(vertxChannel, replyMessage);
          });
        }
        pubsub.send(cmd);
        break;

      case SUBSCRIBE:
        cmd.setExpectedReplies(redisArgs.size());

        for (Object obj : redisArgs) {
          String channel = (String) obj;
          // compose the listening address as base + . + channel
          final String vertxChannel = baseAddress + "." + channel;
          subscriptions.registerChannelSubscribeHandler(channel, (channel1, replyData) -> {
            JsonObject replyMessage = new JsonObject();
            replyMessage.put("status", "ok");
            JsonObject message = new JsonObject();
            message.put("channel", channel1);
            message.put("message", replyData[2].asType(String.class, encoding));
            replyMessage.put("value", message);
            eb.send(vertxChannel, replyMessage);
          });
        }
        pubsub.send(cmd);
        break;

      case PUNSUBSCRIBE:
        // unregister all channels
        if (redisArgs == null || redisArgs.size() == 0) {
          // unsubscribe all
          cmd.setExpectedReplies(subscriptions.patternSize());
          subscriptions.unregisterPatternSubscribeHandler(null);
        } else {
          cmd.setExpectedReplies(redisArgs.size());

          for (Object obj : redisArgs) {
            String pattern = (String) obj;
            subscriptions.unregisterPatternSubscribeHandler(pattern);
          }
        }
        pubsub.send(cmd);
        break;

      case UNSUBSCRIBE:
        // unregister all channels
        if (redisArgs == null || redisArgs.size() == 0) {
          // unsubscribe all
          cmd.setExpectedReplies(subscriptions.channelSize());
          subscriptions.unregisterChannelSubscribeHandler(null);
        } else {
          cmd.setExpectedReplies(redisArgs.size());

          for (Object obj : redisArgs) {
            String channel = (String) obj;
            subscriptions.unregisterChannelSubscribeHandler(channel);
          }
        }
        pubsub.send(cmd);
        break;
      case QUIT:
        // this is a special case that must be sent to all connections
        redis.send(cmd);
        pubsub.send(cmd);
        break;
      default:
        // all other commands are sent to the normal connection
        redis.send(cmd);
        break;
    }
  }

  /**
   * A helper method to package method parameters into JsonArray payload.
   *
   * Null parameters are ignored.
   *
   * @param parameters Call parameters
   * @return JsonArray that can be passed to send()
   */
  @SuppressWarnings("unchecked")
  static List<?> toPayload(Object ... parameters) {
    List<Object> result = new ArrayList<>(parameters.length);

    for (Object param: parameters) {
      // unwrap
      if (param instanceof JsonArray) {
        param = ((JsonArray) param).getList();
      }
      // unwrap
      if (param instanceof JsonObject) {
        param = ((JsonObject) param).getMap();
      }

      if (param instanceof Collection) {
        ((Collection) param).stream().filter(el -> el != null).forEach(result::add);
      } else if (param instanceof Map) {
        for (Map.Entry<?, ?> pair : ((Map<?, ?>) param).entrySet()) {
          result.add(pair.getKey());
          result.add(pair.getValue());
        }
      } else if (param instanceof Stream) {
        ((Stream) param).forEach(e -> {
          if (e instanceof Object[]) {
            for (Object item : (Object[]) e) {
              result.add(item);
            }
          } else {
            result.add(e);
          }
        });
      } else if (param instanceof Buffer) {
        result.add(((Buffer) param).getBytes());
      } else if (param != null) {
        result.add(param);
      }
    }
    return result;
  }

}