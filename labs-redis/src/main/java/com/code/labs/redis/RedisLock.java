package com.code.labs.redis;

import java.io.Closeable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisLock implements Closeable {

  private static final String EXPIRY_TIME_SECONDS = "10";
  private static final int SPIN_THRESHOLD_MILLIS = 300;
  private ThreadLocal<HashMap<String,String>> holdLocks = new ThreadLocal<>();

  public static final String LUA_DELETE =
      "if redis.call(\"GET\", KEYS[1]) == ARGV[1] then\n" +
          "return redis.call(\"DEL\", KEYS[1])\n" +
      "else\n" +
          "return 0\n" +
      "end";

  public static final String LUA_NXEX =
      "if redis.call(\"SETNX\", KEYS[1], ARGV[1]) == 1 then\n" +
          "return redis.call(\"EXPIRE\", KEYS[1], ARGV[2])\n" +
      "else\n" +
          "return 0\n" +
      "end";

  private JedisPool jedisPool;

  public RedisLock(JedisPool jedisPool) {
    this.jedisPool = jedisPool;
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
    final String uuid = localLocks.containsKey(lockName) ? localLocks.get(lockName) : UUID.randomUUID().toString();
    while (timeout > 0) {
      long start = System.currentTimeMillis();
      boolean isNewLock = false;
      try (Jedis jedis = jedisPool.getResource()) {
        isNewLock = (long) jedis.eval(RedisLock.LUA_NXEX, Arrays.asList(lockName),
            Arrays.asList(uuid, EXPIRY_TIME_SECONDS)) > 0;
      }
      if (isNewLock) return true;

      if (localLocks.containsKey(lockName)) {
        String localUuid = localLocks.get(lockName);
        String currentUuid;
        try (Jedis jedis = jedisPool.getResource()) {
          currentUuid = jedis.get(lockName);
        }
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
    HashMap<String,String> localLocks = getLocalLocks();
    if (localLocks.containsKey(lockName)) {
      final String holdUuid = localLocks.get(lockName);
      try (Jedis jedis = jedisPool.getResource()) {
        jedis.eval(LUA_DELETE, Arrays.asList(lockName), Arrays.asList(holdUuid));
      }
      localLocks.remove(lockName);
    }
  }

  @Override
  public void close() {
    if (jedisPool != null) jedisPool.close();
  }
}
