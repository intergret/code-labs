package com.code.labs.redis;

import java.util.List;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;

public class RedisPubSub {

  private RedisTemplate redisTemplate;

  public RedisPubSub(RedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  public void publish(final String channel, final String message) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.publish(channel, message);
      }
    });
  }

  public void subscribe(final String channel, final JedisPubSub jedisPubSub) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.subscribe(jedisPubSub, channel);
      }
    });
  }

  public void psubscribe(final String pattern, final JedisPubSub jedisPubSub) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.psubscribe(jedisPubSub, pattern);
      }
    });
  }

  public void batchPublish(final String channel, final List<String> messages) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        Pipeline p = jedis.pipelined();
        for (String message : messages) {
          p.publish(channel, message);
        }
        p.syncAndReturnAll();
      }
    });
  }
}
