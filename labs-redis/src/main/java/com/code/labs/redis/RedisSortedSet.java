package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisSortedSet implements Closeable {

  private JedisPool jedisPool;

  public RedisSortedSet(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void add(final String key, final double weight, final String element) {
    try (Jedis jedis = jedisPool.getResource()) {
      // store in asc order by default
      jedis.zadd(key, weight, element);
    }
  }

  public List<String> pop(final String key, final long size) {
    final List<String> elements = new ArrayList<>();
    try (Jedis jedis = jedisPool.getResource()) {
      Set<String> elementSet = jedis.zrevrange(key, 0, size - 1);
      if (elementSet != null && elementSet.size() > 0) {
        Pipeline p = jedis.pipelined();
        for (String element : elementSet) {
          if (element != null) {
            elements.add(element);
            p.zrem(key, element);
          }
        }
        p.syncAndReturnAll();
      }
    }
    return elements;
  }

  public long size(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.zcard(key);
    }
  }

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}