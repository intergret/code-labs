package com.code.labs.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisSortedSet {

  private RedisTemplate redisTemplate;

  public RedisSortedSet(RedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  public void add(final String key, final double weight, final String element) {
    redisTemplate.execute(new RedisAction() {
      public void execute(Jedis jedis) {
        // store in desc order by weight
        jedis.zadd(key, -weight, element);
      }
    });
  }

  public List<String> pop(final String key, final long size) {
    final List<String> elements = new ArrayList<>();
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        Set<String> elementSet = jedis.zrange(key, 0, size - 1);
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
    });
    return elements;
  }

  public long size(final String key) {
    return redisTemplate.query(new RedisAction<Long>() {
      public Long query(Jedis jedis) {
        return jedis.zcard(key);
      }
    });
  }
}