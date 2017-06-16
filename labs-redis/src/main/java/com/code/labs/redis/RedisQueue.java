package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisQueue implements Closeable {

  private JedisPool jedisPool;

  public RedisQueue(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void push(final String key, final String element) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.lpush(key, element);
    }
  }

  public void batchPush(final String key, final List<String> elements) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline p = jedis.pipelined();
      for (String element : elements) {
        p.lpush(key, element);
      }
      p.syncAndReturnAll();
    }
  }

  public String pop(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.rpop(key);
    }
  }

  public List<String> batchPop(final String key, final int size) {
    final List<String> elements = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline p = jedis.pipelined();
      for (int i = 0; i < size; i++) {
        p.rpop(key);
      }
      List<Object> objects = p.syncAndReturnAll();
      for (int i = 0; i < objects.size(); i++) {
        if (objects.get(i) != null) {
          elements.add((String) objects.get(i));
        }
      }
    }
    return elements;
  }

  public long size(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.llen(key);
    }
  }

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}
