package com.code.labs.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisCounter {

  private RedisTemplate redisTemplate;

  public RedisCounter(RedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  public void incre(final String key, final long step) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.incrBy(key, step);
      }
    });
  }

  public void batchIncre(final Map<String, Long> keyIncres) {
    if (keyIncres != null && keyIncres.size() > 0) {
      redisTemplate.execute(new RedisAction<Long>() {
        public void execute(Jedis jedis) {
          Pipeline p = jedis.pipelined();
          for (String key : keyIncres.keySet()) {
            p.incrBy(key, keyIncres.get(key));
          }
          p.syncAndReturnAll();
        }
      });
    }
  }

  public Long get(final String key) {
    return redisTemplate.query(new RedisAction<Long>() {
      public Long query(Jedis jedis) {
        try {
          return Long.parseLong(jedis.get(key));
        } catch (NumberFormatException e) {
          throw e;
        }
      }
    });
  }

  public Map<String, Long> batchGet(final List<String> keys) {
    final Map<String, Long> result = new HashMap<>();
    if (keys != null && keys.size() > 0) {
      redisTemplate.execute(new RedisAction<Long>() {
        public void execute(Jedis jedis) {
          Pipeline p = jedis.pipelined();
          for (String key : keys) {
            p.get(key);
          }
          List<Object> resultObjects = p.syncAndReturnAll();
          try {
            for (int i = 0; i < keys.size(); i++) {
              result.put(keys.get(i), Long.parseLong((String) resultObjects.get(i)));
            }
          } catch (NumberFormatException e) {
            throw e;
          }
        }
      });
    }
    return result;
  }

  public void set(final String key, final long count) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.set(key, String.valueOf(count));
      }
    });
  }

  public void batchSet(final Map<String, Long> keyCounts) {
    if (keyCounts != null && keyCounts.size() > 0) {
      redisTemplate.execute(new RedisAction<Long>() {
        public void execute(Jedis jedis) {
          Pipeline p = jedis.pipelined();
          for (String key : keyCounts.keySet()) {
            p.set(key, String.valueOf(keyCounts.get(key)));
          }
          p.syncAndReturnAll();
        }
      });
    }
  }

  public boolean exist(final String key) {
    return redisTemplate.query(new RedisAction<Boolean>() {
      public Boolean query(Jedis jedis) {
        return jedis.exists(key);
      }
    });
  }

  public void delete(final String key) {
    redisTemplate.execute(new RedisAction<String>() {
      public void execute(Jedis jedis) {
        jedis.del(key);
      }
    });
  }

  public void batchDelete(final List<String> keys) {
    if (keys != null && keys.size() > 0) {
      redisTemplate.execute(new RedisAction<String>() {
        public void execute(Jedis jedis) {
          Pipeline p = jedis.pipelined();
          for (String key : keys) {
            p.del(key);
          }
          p.syncAndReturnAll();
        }
      });
    }
  }
}
