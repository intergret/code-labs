package com.code.labs.redis;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import redis.clients.jedis.Jedis;

public class RedisLock {

  private static final int EXPIRY_TIME_SECONDS = 10;
  private static final int SPIN_THRESHOLD_MILLIS = 300;

  private RedisTemplate redisTemplate;

  private ThreadLocal<HashMap<String,String>> holdLocks;

  public RedisLock(RedisTemplate redisTemplate) {
    this.redisTemplate = redisTemplate;
    this.holdLocks = new ThreadLocal<>();
  }

  private HashMap<String,String> getLocalLocks() {
    HashMap<String,String> localHoldLocks = this.holdLocks.get();
    if (localHoldLocks == null) {
      localHoldLocks = new HashMap<>();
      this.holdLocks.set(localHoldLocks);
    }
    return localHoldLocks;
  }

  public boolean acquire(final String lockName, int timeout) {
    final HashMap<String,String> localLocks = getLocalLocks();
    final String uuid = localLocks.containsKey(lockName) ?
        localLocks.get(lockName) : UUID.randomUUID().toString();
    while (timeout > 0) {
      long start = System.currentTimeMillis();
      Boolean isNewLock = redisTemplate.query(new RedisAction<Boolean>() {
        public Boolean query(Jedis jedis) {
          boolean success = jedis.setnx(lockName, uuid) == 1;
          if (success) {
            jedis.expire(lockName, EXPIRY_TIME_SECONDS);
            localLocks.put(lockName, uuid);
          }
          return success;
        }
      });
      if (isNewLock) return true;

      if (localLocks.containsKey(lockName)) {
        String localUuid = localLocks.get(lockName);
        String currentUuid = redisTemplate.query(new RedisAction<String>() {
          public String query(Jedis jedis) {
            return jedis.get(lockName);
          }
        });
        if (localUuid.equals(currentUuid)) {
          return true;
        }
      }
      timeout -= System.currentTimeMillis() - start;
      if (timeout > SPIN_THRESHOLD_MILLIS) {
        long sleepStart = System.currentTimeMillis();
        LockSupport.parkNanos(SPIN_THRESHOLD_MILLIS * 1000000);
        timeout -= System.currentTimeMillis() - sleepStart;
      }
    }

    localLocks.remove(lockName);
    return false;
  }

  public void release(final String lockName) {
    HashMap<String, String> localLocks = getLocalLocks();
    if (localLocks.containsKey(lockName)) {
      final String holdUuid = localLocks.get(lockName);
      redisTemplate.execute(new RedisAction<String>() {
        public void execute(Jedis jedis) {
          String currentUuid = jedis.get(lockName);
          if (holdUuid.equals(currentUuid)) {
            jedis.del(lockName);
          }
        }
      });
      localLocks.remove(lockName);
    }
  }
}
