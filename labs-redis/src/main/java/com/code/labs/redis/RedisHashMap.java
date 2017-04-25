package com.code.labs.redis;

import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisHashMap {

  private RedisTemplate redisTemplate;

  public RedisHashMap(RedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  public String get(final String key, final String field) {
    return redisTemplate.query(new RedisAction<String>() {
      public String query(Jedis jedis) {
        return jedis.hget(key, field);
      }
    });
  }

  public Map<String,String> getAll(final String key) {
    return redisTemplate.query(new RedisAction<Map<String,String>>() {
      public Map<String,String> query(Jedis jedis) {
        return jedis.hgetAll(key);
      }
    });
  }

  public void set(final String key, final String field, final String value) {
    redisTemplate.execute(new RedisAction() {
      public void execute(Jedis jedis) {
        jedis.hset(key, field, value);
      }
    });
  }

  public void batchSet(final String key, final Map<String,String> fieldValues) {
    if (fieldValues == null || fieldValues.size() == 0) {
      return;
    }
    redisTemplate.execute(new RedisAction() {
      public void execute(Jedis jedis) {
        jedis.hmset(key, fieldValues);
      }
    });
  }

  public void delete(final String key, final String field) {
    redisTemplate.execute(new RedisAction() {
      public void execute(Jedis jedis) {
        jedis.hdel(key, field);
      }
    });
  }

  public void batchDelete(final String key, final List<String> fields) {
    redisTemplate.execute(new RedisAction() {
      public void execute(Jedis jedis) {
        Pipeline p = jedis.pipelined();
        for (String field : fields) {
          p.hdel(key, field);
        }
        p.syncAndReturnAll();
      }
    });
  }

  public boolean exists(final String key, final String field) {
    return redisTemplate.query(new RedisAction<Boolean>() {
      public Boolean query(Jedis jedis) {
        return jedis.hexists(key, field);
      }
    });
  }

  public Long size(final String key) {
    return redisTemplate.query(new RedisAction<Long>() {
      public Long query(Jedis jedis) {
        return jedis.hlen(key);
      }
    });
  }
}
