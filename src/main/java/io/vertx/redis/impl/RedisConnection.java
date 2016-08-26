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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.RedisSentinel;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Base class for Redis Vert.x client. Generated client would use the facilities
 * in this class to implement typed commands.
 */
class RedisConnection {

  public static final int RETRY_INTERVAL = 300;
  private final Vertx vertx;

  private final Context context;

  private static final Logger log = LoggerFactory.getLogger(RedisConnection.class);

  /**
   * there are 2 queues, one for commands not yet sent over the wire to redis and another for commands already sent to
   * redis. At start up it expected that until the connection handshake is complete the pending queue will grow and once
   * the handshake completes it will be empty while the second one will be in constant movement.
   *
   * Since the client works **ALWAYS** in pipeline mode the order of adding and removing elements to the queues is
   * crucial. A command is sent only when its reply handler or handlers are added to any of the queues and the command
   * is send to the wire.
   *
   * For this reason we must **ALWAYS** synchronize the access to the queues and writes to the socket.
   */
  // pending: commands that have not yet been sent to the server
  private final Queue<Command<?>> pending = new LinkedList<>();
  // waiting: commands that have been sent but not answered
  private final Queue<Command<?>> waiting = new LinkedList<>();

  private final ReplyParser replyParser;

  private final NetClient client;
  private final RedisOptions config;

  private enum State {
    /**
     * The connection is not active. The is a stop state.
     */
    DISCONNECTED,
    /**
     * The connection is in transit, from here it can become connected or and error can occur.
     */
    CONNECTING,
    /**
     * Connection is active from here it can become an error or disconnected.
     */
    CONNECTED,
    /**
     * Connection problem
     */
    ERROR
  }

  private final AtomicReference<State> state = new AtomicReference<>(State.DISCONNECTED);

  private volatile NetSocket netSocket;

  private SentinelList sentinelList;

  /**
   * Create a RedisConnection.
   */
  public RedisConnection(Vertx vertx, RedisOptions config, RedisSubscriptions subscriptions) {
    this.vertx = vertx;
    this.context = vertx.getOrCreateContext();
    this.config = config;

    // create a netClient for the connection
    client = vertx.createNetClient(new NetClientOptions()
        .setTcpKeepAlive(config.isTcpKeepAlive())
        .setTcpNoDelay(config.isTcpNoDelay())
        .setConnectTimeout(config.getConnectionTimeout()));

    if (subscriptions != null) {
      this.replyParser = new ReplyParser(reply -> {
        // Pub/sub messages are always multi-bulk
        if (reply.is('*')) {
          Reply[] data = (Reply[]) reply.data();
          if (data != null) {
            // message
            if (data.length == 3) {
              if (data[0].is('$') && "message".equals(data[0].asType(String.class))) {
                String channel = data[1].asType(String.class);
                subscriptions.handleChannel(channel, data);
                return;
              }
            }
            // pmessage
            else if (data.length == 4) {
              if (data[0].is('$') && "pmessage".equals(data[0].asType(String.class))) {
                String pattern = data[1].asType(String.class);
                subscriptions.handlePattern(pattern, data);
                return;
              }
            }
          }
        }

        // fallback to normal handler
        handleReply(reply);
      });

    } else {
      this.replyParser = new ReplyParser(this::handleReply);
    }

    // sentinel
    final String master = config.getMaster();
    if (master != null && !master.isEmpty()) {
      // connect via sentinel

      JsonArray sentinelJa = config.getSentinels();
      if (sentinelJa == null || sentinelJa.isEmpty()) {
        throw new RuntimeException("Sentinels is not config!");
      }

      sentinelList = new SentinelList();
      for (int i = 0; i < sentinelJa.size(); ++i) {
        JsonObject jo = sentinelJa.getJsonObject(i);
        sentinelList.add(new SentinelList.SentinelInfo(jo.getString("ip"), jo.getInteger("port")));
      }
    }

  }

  private void connect() {
    if (state.compareAndSet(State.DISCONNECTED, State.CONNECTING)) {
      replyParser.reset();

      if (sentinelList != null) {
        connectViaSentinel(0);
      } else {
        connectWithPortAndHost(config.getPort(), config.getHost(), -1);
      }
    }
  }

  private void connectViaSentinel(int index) {
    if (index >= sentinelList.size()) {
      handleConnectFailed(new RuntimeException("failed to connect sentinels!"));
      retryConnectWithDelay();
      return;
    }

    SentinelList.SentinelInfo info = sentinelList.get(index);
    RedisSentinel sentinel = RedisSentinel.create(vertx, new RedisOptions().setPort(info.getPort()).setHost(info.getHost()).setConnectTimeout(RETRY_INTERVAL));
    sentinel.getMasterAddrByName(config.getMaster(), event -> {
      if (event.failed()) {
        // try next one
        connectViaSentinel(index + 1);
      } else {
        connectWithPortAndHost(event.result().getInteger(1), event.result().getString(0), index);
      }
    });

  }

  private void handleConnectFailed(Throwable cause) {
    runOnContext(v -> {
      if (state.compareAndSet(State.CONNECTING, State.ERROR)) {
        // clean up any waiting command
        clearQueue(waiting, cause);
        // clean up any pending command
        clearQueue(pending, cause);

        // close the socket if previously connected
        if (netSocket != null) {
          netSocket.close();
        }

        state.set(State.DISCONNECTED);
      }
    });

  }

  private void connectWithPortAndHost(int port, String host, int sentinelIndex) {
    client.connect(port, host, asyncResult -> {
      if (asyncResult.failed()) {
        handleConnectFailed(asyncResult.cause());
      } else {
        netSocket = asyncResult.result()
            .handler(replyParser)
            .closeHandler(v -> runOnContext(v0 -> {
              state.set(State.ERROR);
              // clean up any waiting command
              clearQueue(waiting, "Connection closed");
              // clean up any pending command
              clearQueue(pending, "Connection closed");

              state.set(State.DISCONNECTED);
            }))
            .exceptionHandler(e -> runOnContext(v0 -> {
              state.set(State.ERROR);
              // clean up any waiting command
              clearQueue(waiting, e);
              // clean up any pending command
              clearQueue(pending, e);

              netSocket.close();
              state.set(State.DISCONNECTED);
            }));

        runOnContext(v -> {
          // clean up any waiting command
          clearQueue(waiting, "Connection lost");

          // handle the connection handshake
          doAuth(sentinelIndex);
        });
      }
    });
  }


  void disconnect(Handler<AsyncResult<Void>> closeHandler) {
    switch (state.get()) {
      case CONNECTING:
        // eventually will become connected
      case CONNECTED:
        final Command<Void> cmd = new Command<>(context, RedisCommand.QUIT, null, Charset.defaultCharset(), ResponseTransform.NONE, Void.class);

        cmd.handler(v -> {
          // at this we force the state to error so any incoming command will not start a connection
          runOnContext(v0 -> {
            if (state.compareAndSet(State.CONNECTED, State.ERROR)) {
              // clean up any waiting command
              clearQueue(waiting, "Connection closed");
              // clean up any pending command
              clearQueue(pending, "Connection closed");

              netSocket.close();
              state.set(State.DISCONNECTED);

              closeHandler.handle(Future.succeededFuture());
            }
          });
        });

        send(cmd);
        break;

      case ERROR:
        // eventually will become DISCONNECTED
      case DISCONNECTED:
        closeHandler.handle(Future.succeededFuture());
        break;
    }
  }

  /**
   * Sends a message to redis, if the connection is not active then the command is queued for processing and the
   * procedure to start a connection is started.
   * <p>
   * While this procedure is going on (CONNECTING) incomming commands are queued.
   *
   * @param command the redis command to send
   */
  void send(final Command<?> command) {
    // start the handshake if not connected
    if (state.get() == State.DISCONNECTED) {
      connect();
    }

    // write to the socket in the netSocket context
    runOnContext(v -> {
      switch (state.get()) {
        case CONNECTED:
          // The order read must match the order written, vertx guarantees
          // that this is only called from a single thread.
          for (int i = 0; i < command.getExpectedReplies(); ++i) {
            waiting.add(command);
          }

          command.writeTo(netSocket);
          break;
        case CONNECTING:
        case ERROR:
        case DISCONNECTED:
          if (state.get() != State.CONNECTED) {
            pending.add(command);
          } else {
            // state changed so start over...
            send(command);
          }
          break;
      }
    });
  }

  /**
   * Once a socket connection is established one needs to authenticate if there is a password
   * @param sentinelIndex
   */
  private void doAuth(int sentinelIndex) {
    if (config.getAuth() != null) {
      // we need to authenticate first
      final List<Object> args = new ArrayList<>();
      args.add(config.getAuth());

      Command<String> authCmd = new Command<>(context, RedisCommand.AUTH, args, Charset.forName(config.getEncoding()), ResponseTransform.NONE, String.class).handler(auth -> {
        if (auth.failed()) {
          // clean up any waiting command
          clearQueue(pending, auth.cause());
          netSocket.close();
          state.set(State.DISCONNECTED);
        } else {
          // auth success, proceed with role
          doRole(sentinelIndex);
        }
      });

      // write to the socket in the netSocket context
      runOnContext(v -> {
        // queue it
        waiting.add(authCmd);
        authCmd.writeTo(netSocket);
      });
    } else {
      // no auth, proceed with role
      doRole(sentinelIndex);
    }
  }

  private void doRole(int sentinelIndex) {

    if (sentinelList == null) {
      doSelect();
      return;
    }

    Command<JsonArray> roleCmd = new Command<>(context, RedisCommand.ROLE, null, Charset.forName(config.getEncoding()), ResponseTransform.NONE, JsonArray.class).handler(res -> {
      boolean ok;
      if (res.failed()) {
        ok = false;
      } else {
        if (res.result().getString(0).equals("master")) {
          ok = true;
        } else {
          ok = false;
        }
      }

      if (!ok) {
        // clean up any waiting command
        clearQueue(pending, res.cause());
        netSocket.close();
        state.set(State.DISCONNECTED);

        retryConnectWithDelay();

      } else {
        //  success, proceed with select
        if (sentinelList != null) {
          sentinelList.moveSentinelToFront(sentinelIndex);
        }
        doSelect();
      }
    });

    // write to the socket in the netSocket context
    runOnContext(v -> {
      // queue it
      waiting.add(roleCmd);
      roleCmd.writeTo(netSocket);
    });

  }

  private void retryConnectWithDelay() {
    this.vertx.setTimer(RETRY_INTERVAL, event -> connect());
  }

  private void doSelect() {
    // optionally there could be a select command
    if (config.getSelect() != null) {

      final List<Object> args = new ArrayList<>();
      args.add(config.getSelect());

      Command<String> selectCmd = new Command<>(context, RedisCommand.SELECT, args, Charset.forName(config.getEncoding()), ResponseTransform.NONE, String.class).handler(select -> {
        if (select.failed()) {
          // clean up any waiting command
          clearQueue(pending, select.cause());

          netSocket.close();
          state.set(State.DISCONNECTED);
        } else {
          // select success, proceed with resend
          resendPending();
        }
      });

      // write to the socket in the netSocket context
      runOnContext(v -> {
        // queue it
        waiting.add(selectCmd);
        selectCmd.writeTo(netSocket);
      });
    } else {
      // no select, proceed with resend
      resendPending();
    }
  }

  private void resendPending() {
    runOnContext(v -> {
      Command<?> command;
      if (state.compareAndSet(State.CONNECTING, State.CONNECTED)) {
        // we are connected so clean up the pending queue
        while ((command = pending.poll()) != null) {
          // The order read must match the order written, vertx guarantees
          // that this is only called from a single thread.
          for (int i = 0; i < command.getExpectedReplies(); ++i) {
            waiting.add(command);
          }

          // write to the socket in the netSocket context
          command.writeTo(netSocket);
        }
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void handleReply(Reply reply) {

    runOnContext(v -> {
      final Command cmd = waiting.poll();

      if (cmd != null) {
        switch (reply.type()) {
          case '-': // Error
            cmd.handle(Future.failedFuture(reply.asType(String.class)));
            return;
          case '+':   // Status
            switch (cmd.responseTransform()) {
              case ARRAY:
                cmd.handle(Future.succeededFuture(new JsonArray().add(reply.asType(String.class))));
                break;
              default:
                cmd.handle(Future.succeededFuture(reply.asType(cmd.returnType())));
                break;
            }
            return;
          case '$':  // Bulk
            switch (cmd.responseTransform()) {
              case ARRAY:
                cmd.handle(Future.succeededFuture(new JsonArray().add(reply.asType(String.class, cmd.encoding()))));
                break;
              case INFO:
                String info = reply.asType(String.class, cmd.encoding());

                if (info == null) {
                  cmd.handle(Future.succeededFuture(null));
                } else {
                  String lines[] = info.split("\\r?\\n");
                  JsonObject value = new JsonObject();

                  JsonObject section = null;
                  for (String line : lines) {
                    if (line.length() == 0) {
                      // end of section
                      section = null;
                      continue;
                    }

                    if (line.charAt(0) == '#') {
                      // begin section
                      section = new JsonObject();
                      // create a sub key with the section name
                      value.put(line.substring(2).toLowerCase(), section);
                    } else {
                      // entry in section
                      int split = line.indexOf(':');
                      if (section == null) {
                        value.put(line.substring(0, split), line.substring(split + 1));
                      } else {
                        section.put(line.substring(0, split), line.substring(split + 1));
                      }
                    }
                  }
                  cmd.handle(Future.succeededFuture(value));
                }
                break;
              default:
                cmd.handle(Future.succeededFuture(reply.asType(cmd.returnType(), cmd.encoding())));
                break;
            }
            return;
          case '*': // Multi
            switch (cmd.responseTransform()) {
              case HASH:
                cmd.handle(Future.succeededFuture(reply.asType(JsonObject.class, cmd.encoding())));
                break;
              default:
                cmd.handle(Future.succeededFuture(reply.asType(JsonArray.class, cmd.encoding())));
                break;
            }
            return;
          case ':':   // Integer
            switch (cmd.responseTransform()) {
              case ARRAY:
                cmd.handle(Future.succeededFuture(new JsonArray().add(reply.asType(Long.class))));
                break;
              default:
                cmd.handle(Future.succeededFuture(reply.asType(cmd.returnType())));
                break;
            }
            return;
          default:
            cmd.handle(Future.failedFuture("Unknown message type"));
        }
      } else {
        log.error("No handler waiting for message: " + reply.asType(String.class));
      }
    });
  }

  private void runOnContext(Handler<Void> handler) {
    if (Vertx.currentContext() == context) {
      handler.handle(null);
    } else {
      context.runOnContext(handler);
    }
  }

  private static void clearQueue(Queue<Command<?>> q, String message) {
    Command<?> cmd;

    // clean up any pending command
    while ((cmd = q.poll()) != null) {
      cmd.handle(Future.failedFuture(message));
    }
  }

  private static void clearQueue(Queue<Command<?>> q, Throwable cause) {
    Command<?> cmd;

    // clean up any pending command
    while ((cmd = q.poll()) != null) {
      cmd.handle(Future.failedFuture(cause));
    }
  }
}