package com.code.labs.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisTemplate {

  private JedisPool jedisPool;

  public RedisTemplate(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
  }

  public <T> T query(RedisAction<T> action) {
    Jedis jedis = jedisPool.getResource();
    try {
      return action.query(jedis);
    } finally {
      if (jedis != null) {
        jedisPool.returnResource(jedis);
      }
    }
  }

  public <T> void execute(RedisAction<T> redisTemplateAction) {
    Jedis jedis = jedisPool.getResource();
    try {
      redisTemplateAction.execute(jedis);
    } finally {
      if (jedis != null) {
        jedisPool.returnResource(jedis);
      }
    }
  }
}
