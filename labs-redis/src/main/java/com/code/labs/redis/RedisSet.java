package com.code.labs.redis;

import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisSet {

  private RedisTemplate redisTemplate;

  public RedisSet(RedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  public void add(final String key, final String member) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.sadd(key, member);
      }
    });
  }

  public void batchAdd(final String key, final List<String> members) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        Pipeline p = jedis.pipelined();
        for (String member : members) {
          p.sadd(key, member);
        }
        p.syncAndReturnAll();
      }
    });
  }

  public void remove(final String key, final String member) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.srem(key, member);
      }
    });
  }

  public void batchRemove(final String key, final List<String> members) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        Pipeline p = jedis.pipelined();
        for (String member : members) {
          p.srem(key, member);
        }
        p.syncAndReturnAll();
      }
    });
  }

  public boolean exist(final String key, final String member) {
    return redisTemplate.query(new RedisAction<Boolean>() {
      public Boolean query(Jedis jedis) {
        return jedis.sismember(key, member);
      }
    });
  }

  public Set<String> list(final String key) {
    return redisTemplate.query(new RedisAction<Set<String>>() {
      public Set<String> query(Jedis jedis) {
        return jedis.smembers(key);
      }
    });
  }

  public long size(final String key) {
    return redisTemplate.query(new RedisAction<Long>() {
      public Long query(Jedis jedis) {
        return jedis.scard(key);
      }
    });
  }
}
