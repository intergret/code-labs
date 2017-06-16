package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisHashMap implements Closeable {

  private JedisPool jedisPool;

  public RedisHashMap(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public String get(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hget(key, field);
    }
  }

  public Map<String,String> getAll(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hgetAll(key);
    }
  }

  public void set(final String key, final String field, final String value) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.hset(key, field, value);
    }
  }

  public void batchSet(final String key, final Map<String,String> fieldValues) {
    if (fieldValues == null || fieldValues.size() == 0) {
      return;
    }
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.hmset(key, fieldValues);
    }
  }

  public void delete(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.hdel(key, field);
    }
  }

  public void batchDelete(final String key, final List<String> fields) {
    try (Jedis jedis = jedisPool.getResource()) {
      Pipeline p = jedis.pipelined();
      for (String field : fields) {
        p.hdel(key, field);
      }
      p.syncAndReturnAll();
    }
  }

  public boolean exists(final String key, final String field) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hexists(key, field);
    }
  }

  public Long size(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.hlen(key);
    }
  }

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}
