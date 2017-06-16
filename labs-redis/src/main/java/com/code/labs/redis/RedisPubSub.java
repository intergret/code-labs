package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

public class RedisPubSub implements Closeable {

  private JedisPool jedisPool;

  public RedisPubSub(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void publish(final String channel, final String message) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.publish(channel, message);
    }
  }

  public void subscribe(final String channel, final JedisPubSub jedisPubSub) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.subscribe(jedisPubSub, channel);
    }
  }

  public void psubscribe(final String pattern, final JedisPubSub jedisPubSub) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.psubscribe(jedisPubSub, pattern);
    }
  }

  public void batchPublish(final String channel, final List<String> messages) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline p = jedis.pipelined();
      for (String message : messages) {
        p.publish(channel, message);
      }
      p.syncAndReturnAll();
    }
  }

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}
