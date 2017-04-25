package com.code.labs.redis;

import redis.clients.jedis.Jedis;

public abstract class RedisAction<T> {

  public T query(Jedis jedis) {
    return null;
  }

  public void execute(Jedis jedis) {}

}
