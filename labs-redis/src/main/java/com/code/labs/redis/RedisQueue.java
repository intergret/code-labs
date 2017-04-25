package com.code.labs.redis;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisQueue {

  private RedisTemplate redisTemplate;

  public RedisQueue(RedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  public void push(final String key, final String element) {
    redisTemplate.execute(new RedisAction() {
      public void execute(Jedis jedis) {
        jedis.lpush(key, element);
      }
    });
  }

  public void batchPush(final String key, final List<String> elements) {
    redisTemplate.execute(new RedisAction() {
      public void execute(Jedis jedis) {
        Pipeline p = jedis.pipelined();
        for (String element : elements) {
          p.lpush(key, element);
        }
        p.syncAndReturnAll();
      }
    });
  }

  public String pop(final String key) {
    return redisTemplate.query(new RedisAction<String>() {
      public String query(Jedis jedis) {
        return jedis.rpop(key);
      }
    });
  }

  public List<String> batchPop(final String key, final int size) {
    final List<String> elements = new ArrayList<>();
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        Pipeline p = jedis.pipelined();
        for (int i = 0; i < size; i++) {
          p.rpop(key);
        }
        List<Object> objects = p.syncAndReturnAll();
        for (int i = 0; i < objects.size(); i++) {
          if (objects.get(i) != null) {
            elements.add((String) objects.get(i));
          }
        }
      }
    });
    return elements;
  }

  public long size(final String key) {
    return redisTemplate.query(new RedisAction<Long>() {
      public Long query(Jedis jedis) {
        return jedis.llen(key);
      }
    });
  }
}
