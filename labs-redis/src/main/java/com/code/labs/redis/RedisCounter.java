package com.code.labs.redis;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

public class RedisCounter implements Closeable {

  private JedisPool jedisPool;

  public RedisCounter(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public void incre(final String key, final long step) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.incrBy(key, step);
    }
  }

  public void batchIncre(final Map<String,Long> keyIncres) {
    if (keyIncres != null && keyIncres.size() > 0) {
      try (Jedis jedis = jedisPool.getResource()) {
        Pipeline p = jedis.pipelined();
        for (String key : keyIncres.keySet()) {
          p.incrBy(key, keyIncres.get(key));
        }
        p.syncAndReturnAll();
      }
    }
  }

  public Long get(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return Long.parseLong(jedis.get(key));
    }
  }

  public Map<String,Long> batchGet(final List<String> keys) {
    final Map<String,Long> result = new HashMap<>();
    if (keys != null && keys.size() > 0) {
      try (Jedis jedis = jedisPool.getResource()) {
        Pipeline p = jedis.pipelined();
        for (String key : keys) {
          p.get(key);
        }
        List<Object> resultObjects = p.syncAndReturnAll();
        for (int i = 0; i < keys.size(); i++) {
          result.put(keys.get(i), Long.parseLong((String) resultObjects.get(i)));
        }
      }
    }
    return result;
  }

  public void set(final String key, final long count) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.set(key, String.valueOf(count));
    }
  }

  public void batchSet(final Map<String,Long> keyCounts) {
    if (keyCounts != null && keyCounts.size() > 0) {
      try (Jedis jedis = jedisPool.getResource()) {
        Pipeline p = jedis.pipelined();
        for (String key : keyCounts.keySet()) {
          p.set(key, String.valueOf(keyCounts.get(key)));
        }
        p.syncAndReturnAll();
      }
    }
  }

  public boolean exist(final String key) {
    try (Jedis jedis = jedisPool.getResource()) {
      return jedis.exists(key);
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

  @Override
  public void close() throws IOException {
    if (jedisPool != null) jedisPool.close();
  }
}
