package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisCache implements Closeable {

  private JedisPool jedisPool;

  public RedisCache(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public String get(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.get(key);
    }
  }

  public Map<String,String> batchGet(final List<String> keys) {
    final Map<String,String> result = new HashMap<>();
    if (keys != null && keys.size() > 0) {
      try (Jedis jedis = jedisPool.getResource()) {
        Pipeline p = jedis.pipelined();
        for (String key : keys) {
          p.get(key);
        }
        List<Object> resultObjects = p.syncAndReturnAll();
        for (int i = 0; i < keys.size(); i++) {
          result.put(keys.get(i), (String) resultObjects.get(i));
        }
      }
    }
    return result;
  }

  public void set(final String key, final String value) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.set(key, value);
    }
  }

  public void batchSet(final Map<String,String> keyValues) {
    if (keyValues != null && keyValues.size() > 0) {
      try (Jedis jedis = jedisPool.getResource()) {
        Pipeline p = jedis.pipelined();
        for (String key : keyValues.keySet()) {
          p.set(key, keyValues.get(key));
        }
        p.syncAndReturnAll();
      }
    }
  }

  public void set(final String key, final String value, final int exp) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.setex(key, exp, value);
    }
  }

  public void batchSet(final Map<String,String> keyValues, final int exp) {
    if (keyValues != null && keyValues.size() > 0) {
      try (Jedis jedis = jedisPool.getResource()) {
        Pipeline p = jedis.pipelined();
        for (String key : keyValues.keySet()) {
          p.setex(key, exp, keyValues.get(key));
        }
        p.syncAndReturnAll();
      }
    }
  }

  public void delete(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.del(key);
    }
  }

  public void batchDelete(final List<String> keys) {
    if (keys != null && keys.size() > 0) {
      try (Jedis jedis = jedisPool.getResource()) {
        Pipeline p = jedis.pipelined();
        for (String key : keys) {
          p.del(key);
        }
        p.syncAndReturnAll();
      }
    }
  }

  public boolean exist(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(key.getBytes());
    }
  }

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}
