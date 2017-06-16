package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisSet implements Closeable {

  private JedisPool jedisPool;

  public RedisSet(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void add(final String key, final String member) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.sadd(key, member);
    }
  }

  public void batchAdd(final String key, final List<String> members) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline p = jedis.pipelined();
      for (String member : members) {
        p.sadd(key, member);
      }
      p.syncAndReturnAll();
    }
  }

  public void remove(final String key, final String member) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.srem(key, member);
    }
  }

  public void batchRemove(final String key, final List<String> members) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline p = jedis.pipelined();
      for (String member : members) {
        p.srem(key, member);
      }
      p.syncAndReturnAll();
    }
  }

  public boolean exist(final String key, final String member) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.sismember(key, member);
    }
  }

  public Set<String> list(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.smembers(key);
    }
  }

  public long size(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.scard(key);
    }
  }

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}
